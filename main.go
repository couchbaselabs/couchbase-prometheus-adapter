package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"

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

type multiError struct {
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
	fmt.Print("REQUEST")
	w.WriteHeader(200)
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
