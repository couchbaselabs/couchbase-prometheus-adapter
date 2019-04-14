package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"

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

func (ca *CouchbaseAdapter) handleWrite(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte{})
}

func (ca *CouchbaseAdapter) handleRead(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte{})
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

	fmt.Println(config.Username)

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
	router.HandleFunc("/read", ca.handleWrite)

	return router
}
