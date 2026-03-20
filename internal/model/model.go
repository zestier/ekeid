package model

import "time"

// LookupResult is returned by API lookups.
type LookupResult struct {
	WikidataID int64
	Mappings   map[int][]string // property ID → values
}

// HealthInfo holds lightweight health data (no expensive COUNT queries).
type HealthInfo struct {
	DatabaseSize  int64
	DumpTime      time.Time
	LastEventSync time.Time
	State         string
}

// DBStats holds database statistics (expensive COUNT queries).
type DBStats struct {
	MappingCount  int64
	PendingCount  int64
	FailedCount   int64
	DatabaseSize  int64
	DumpTime      time.Time
	LastEventSync time.Time
	State         string
}
