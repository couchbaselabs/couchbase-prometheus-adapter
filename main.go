package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/common/model"

	"github.com/google/uuid"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"

	"github.com/couchbase/gocb"
	"github.com/gorilla/mux"
	"github.com/spf13/viper"
)

const bucketName = "couchbase.bucket"
const username = "couchbase.username"
const password = "couchbase.password"
const connStr = "couchbase.connString"
const listenAddr = "http.listen"

type Config struct {
	ConnStr    string `yaml:"couchbase.connString"`
	Username   string `yaml:"couchbase.username"`
	Password   string `yaml:"couchbase.password"`
	BucketName string `yaml:"couchbase.bucket"`
	ListenAddr string `yaml:"http.listen"`
}

func (cfg *Config) AddFlags(flagSet *flag.FlagSet) {
}

func (cfg *Config) InitFromViper(v *viper.Viper) {
	cfg.ConnStr = v.GetString(connStr)
	cfg.Username = v.GetString(username)
	cfg.Password = v.GetString(password)
	cfg.BucketName = v.GetString(bucketName)
	cfg.ListenAddr = v.GetString(listenAddr)
}

type StorageAdapter interface {
	Store(key string, sample Sample) error
	Read(query string, params []interface{}) ([]Sample, error)
	BucketName() string
}

type Adapter struct {
	storageAdapter StorageAdapter
}

type Sample struct {
	Metric    map[string]string `json:"metric"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
}

var (
	writeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "couchbase_adapter_write_latency_seconds",
		Help: "How long it took us to respond to write requests.",
	})
	writeSamples = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "couchbase_adapter_write_timeseries_samples",
		Help: "How many samples each written timeseries has.",
	})
	writeCBDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "couchbase_adapter_write_couchbase_latency_seconds",
		Help: "Latency for inserts to Couchbase Server.",
	})
	writeCBErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "couchbase_adapter_write_couchbase_failed_total",
		Help: "How many inserts to Couchbase Server failed.",
	})
	readDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "couchbase_adapter_read_latency_seconds",
		Help: "How long it took us to respond to read requests.",
	})
	readCBDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "couchbase_adapter_read_couchbase_latency_seconds",
		Help: "Latency for selects from Crate.",
	})
	readCBErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "couchbase_adapter_read_couchbase_failed_total",
		Help: "How many selects from Crate failed.",
	})
	readSamples = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "couchbase_adapter_read_timeseries_samples",
		Help: "How many samples each returned timeseries has.",
	})
)

func init() {
	prometheus.MustRegister(writeDuration)
	prometheus.MustRegister(writeSamples)
	prometheus.MustRegister(writeCBDuration)
	prometheus.MustRegister(writeCBErrors)
	prometheus.MustRegister(readDuration)
	prometheus.MustRegister(readSamples)
	prometheus.MustRegister(readCBDuration)
	prometheus.MustRegister(readCBErrors)
}

func (ca *Adapter) handleWrite(w http.ResponseWriter, r *http.Request) {
	timer := prometheus.NewTimer(writeDuration)
	defer timer.ObserveDuration()
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var errs []string
	for _, ts := range req.Timeseries {
		metric := make(map[string]string, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[l.Name] = l.Value
		}

		for _, sample := range ts.Samples {
			uid, err := uuid.NewRandom()
			if err != nil {
				errs = append(errs, err.Error())
				continue
			}

			writeTimer := prometheus.NewTimer(writeCBDuration)
			// the key is pretty irrelevant, so long as it's unique then we're good
			err = ca.storageAdapter.Store(uid.String(), Sample{
				Metric:    metric,
				Timestamp: sample.Timestamp,
				Value:     sample.Value,
			})
			writeTimer.ObserveDuration()
			if err != nil {
				writeCBErrors.Inc()
				errs = append(errs, err.Error())
			}
		}
		writeSamples.Observe(float64(len(ts.Samples)))
	}

	if len(errs) > 0 {
		http.Error(w, strings.Join(errs, ", "), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(200)
}

func (ca *Adapter) processQuery(query *prompb.Query) ([]*prompb.TimeSeries, error) {
	selectors := make([]string, 0, len(query.Matchers)+2)
	params := make([]interface{}, 0, len(query.Matchers)+2)
	for _, matcher := range query.Matchers {
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			selectors = append(selectors, fmt.Sprintf("metric.%s=?", matcher.Name))
		case prompb.LabelMatcher_NEQ:
			selectors = append(selectors, fmt.Sprintf("metric.%s!=?", matcher.Name))
		case prompb.LabelMatcher_RE:
			selectors = append(selectors, fmt.Sprintf("REGEX_CONTAINS(metric.%s, ?)", matcher.Name))
		case prompb.LabelMatcher_NRE:
			selectors = append(selectors, fmt.Sprintf("NOT REGEX_CONTAINS(metric.%s, ?)", matcher.Name))
		}
		params = append(params, matcher.Value)
	}
	selectors = append(selectors, fmt.Sprintf("timestamp BETWEEN %d AND %d", query.StartTimestampMs, query.EndTimestampMs))

	q := fmt.Sprintf("SELECT `metric`, `timestamp`, `value` from `%s` WHERE %s", ca.storageAdapter.BucketName(),
		strings.Join(selectors, " AND "))
	timer := prometheus.NewTimer(readCBDuration)
	samples, err := ca.storageAdapter.Read(q, params)
	timer.ObserveDuration()
	if err != nil {
		readCBErrors.Inc()
		return nil, err
	}

	timeseries := map[string]*prompb.TimeSeries{}
	for _, sample := range samples {
		metric := model.Metric{}
		for k, v := range sample.Metric {
			metric[model.LabelName(k)] = model.LabelValue(v)
		}

		ts, ok := timeseries[metric.String()]
		if !ok {
			ts = &prompb.TimeSeries{}
			for k, v := range sample.Metric {
				ts.Labels = append(ts.Labels, prompb.Label{Name: k, Value: v})
			}
			timeseries[metric.String()] = ts
		}
		ts.Samples = append(ts.Samples, prompb.Sample{Timestamp: sample.Timestamp, Value: sample.Value})
	}

	resp := make([]*prompb.TimeSeries, 0, len(timeseries))
	for k, v := range timeseries {
		resp = append(resp, v)
		readSamples.Observe(float64(len(timeseries[k].Samples)))
	}

	return resp, nil
}

func (ca *Adapter) processQueries(queries []*prompb.Query) ([]*prompb.TimeSeries, error) {
	return nil, nil
}

func (ca *Adapter) handleRead(w http.ResponseWriter, r *http.Request) {
	timer := prometheus.NewTimer(readDuration)
	defer timer.ObserveDuration()
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := ca.processQuery(req.Queries[0])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{Timeseries: result},
		},
	}
	data, err := proto.Marshal(&resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	if _, err := w.Write(snappy.Encode(nil, data)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func main() {
	gocb.SetLogger(gocb.DefaultStdioLogger())
	var configPath string
	flag.StringVar(&configPath, "config", "", "A path to the plugin's configuration file")
	flag.Parse()

	v := viper.New()
	if configPath != "" {
		v.SetConfigFile(configPath)
	}

	v.SetDefault(bucketName, "default")
	v.SetDefault(connStr, "couchbase://localhost")

	if configPath != "" {
		err := v.ReadInConfig()
		if err != nil {
			log.Println(err)
			panic(err)
		}
	}

	var config Config
	config.InitFromViper(v)

	storageAdapter, err := NewCouchbaseAdapter(config.ConnStr, config.Username, config.Password, config.BucketName)
	if err != nil {
		log.Println(err)
		panic(err)
	}

	ca := &Adapter{
		storageAdapter: storageAdapter,
	}

	stop := make(chan os.Signal, 1)

	// Stop the server on interrupt
	signal.Notify(stop, os.Interrupt)

	r := routes(ca)
	srv := &http.Server{Addr: config.ListenAddr, Handler: r}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatalf("Error running server: %s", err)
		}
	}()

	log.Println("Server running on", srv.Addr)
	<-stop
	log.Println("Stopping server")
	srv.Shutdown(nil)
}

func routes(ca *Adapter) *mux.Router {
	router := mux.NewRouter()

	router.HandleFunc("/write", ca.handleWrite)
	router.HandleFunc("/read", ca.handleRead)

	return router
}
