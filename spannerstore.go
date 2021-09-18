package dstore

import spanner "cloud.google.com/go/spanner/apiv1"

// SpannerStore implements Storer with Cloud Spanner as storage.
type SpannerStore struct {
	Client *spanner.Client
	Table  string
}

// Put saves a key/value to Spanner.
func (s *SpannerStore) Put(kv KeyValue) error {
	return nil
}

// Get reads a key (or keys) from Spanner.
func (s *SpannerStore) Get(key string, limit ...int64) ([]KeyValue, error) {
	return nil, nil
}
