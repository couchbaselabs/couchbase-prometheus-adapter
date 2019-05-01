package main

import (
	"gopkg.in/couchbase/gocb.v1"
)

type CouchbaseAdapter struct {
	Cluster *gocb.Cluster
	Bucket  *gocb.Bucket
}

func NewCouchbaseAdapter(connStr, username, password, bucketName string) (*CouchbaseAdapter, error) {
	cluster, err := gocb.Connect(connStr)
	if err != nil {
		return nil, err
	}

	err = cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	})
	if err != nil {
		return nil, err
	}

	bucket, err := cluster.OpenBucket(bucketName, "")
	if err != nil {
		return nil, err
	}

	return &CouchbaseAdapter{
		Cluster: cluster,
		Bucket:  bucket,
	}, nil
}

func (ca *CouchbaseAdapter) Store(key string, sample Sample) error {
	_, err := ca.Bucket.Upsert(key, sample, 0)

	return err
}

func (ca *CouchbaseAdapter) Read(query string, params []interface{}) ([]Sample, error) {
	q := gocb.NewAnalyticsQuery(query)
	res, err := ca.Bucket.ExecuteAnalyticsQuery(q, params)
	if err != nil {
		return nil, err
	}

	var samples []Sample
	var sample Sample
	for res.Next(&sample) {
		samples = append(samples, sample)
	}

	if err := res.Close(); err != nil {
		return nil, err
	}

	return samples, nil
}

func (ca *CouchbaseAdapter) BucketName() string {
	return ca.Bucket.Name()
}
