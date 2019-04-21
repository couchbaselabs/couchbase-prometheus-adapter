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

type CouchbaseAdapter struct {
	Cluster *gocb.Cluster
	Bucket  *gocb.Bucket
}

type Sample struct {
	Metric    map[string]string `json:"metric"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
}

func (ca *CouchbaseAdapter) handleWrite(w http.ResponseWriter, r *http.Request) {
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

	var errs []error
	for _, ts := range req.Timeseries {
		metric := make(map[string]string, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[l.Name] = l.Value
		}

		for _, sample := range ts.Samples {
			uid, err := uuid.NewRandom()
			if err != nil {
				errs = append(errs, err)
			}

			// the key is pretty irrelevant, so long as it's unique then we're good
			_, err = ca.Bucket.Upsert(uid.String(), Sample{
				Metric:    metric,
				Timestamp: sample.Timestamp,
				Value:     sample.Value,
			}, 0)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	w.WriteHeader(200)
}

func (ca *CouchbaseAdapter) processQuery(query *prompb.Query) ([]*prompb.TimeSeries, error) {
	selectors := make([]string, 0, len(query.Matchers)+2)
	params := make([]interface{}, 0, len(query.Matchers)+2)
	for _, matcher := range query.Matchers {
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			selectors = append(selectors, fmt.Sprintf("\"%s\"=?", matcher.Name))
		case prompb.LabelMatcher_NEQ:
			selectors = append(selectors, fmt.Sprintf("\"%s\"!=?", matcher.Name))
		case prompb.LabelMatcher_RE:
			selectors = append(selectors, fmt.Sprintf("REGEX_CONTAINS(\"%s\", ?)", matcher.Name))
		case prompb.LabelMatcher_NRE:
			selectors = append(selectors, fmt.Sprintf("NOT REGEX_CONTAINS(\"%s\", ?)", matcher.Name))
		}
		params = append(params, matcher.Value)
	}
	selectors = append(selectors, fmt.Sprintf("\"timestamp\" BETWEEN %d AND %d", query.StartTimestampMs, query.EndTimestampMs))

	n1ql := gocb.NewN1qlQuery(fmt.Sprintf("SELECT `metric`, `timestamp`, `value` from `%s` WHERE %s", ca.Bucket.Name(),
		strings.Join(selectors, " AND ")))
	res, err := ca.Bucket.ExecuteN1qlQuery(n1ql, params)
	if err != nil {
		return nil, err
	}

	timeseries := map[string]*prompb.TimeSeries{}
	var sample Sample
	for res.Next(&sample) {
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
	for _, v := range timeseries {
		resp = append(resp, v)
	}

	return resp, nil
}

func (ca *CouchbaseAdapter) processQueries(queries []*prompb.Query) ([]*prompb.TimeSeries, error) {
	return nil, nil
}

func (ca *CouchbaseAdapter) handleRead(w http.ResponseWriter, r *http.Request) {
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
			fmt.Println(err)
			panic(err)
		}
	}

	var config Config
	config.InitFromViper(v)

	// gocb.SetLogger(gocb.VerboseStdioLogger())
	cluster, err := gocb.Connect(config.ConnStr)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: config.Username,
		Password: config.Password,
	})
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	bucket, err := cluster.OpenBucket(config.BucketName, "")
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	ca := &CouchbaseAdapter{
		Cluster: cluster,
		Bucket:  bucket,
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

	fmt.Println("Server running on", srv.Addr)
	<-stop
	log.Println("Stopping server")
	srv.Shutdown(nil)
}

func routes(ca *CouchbaseAdapter) *mux.Router {
	router := mux.NewRouter()

	router.HandleFunc("/write", ca.handleWrite)
	router.HandleFunc("/read", ca.handleRead)

	return router
}
