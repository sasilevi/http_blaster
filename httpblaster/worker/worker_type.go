package worker

//Type : worker type enum
type Type int32

const (
	// Performance : worker for performance load testing
	Performance Type = iota
	// Ingestion : worker for data ingestion testing
	Ingestion
)
