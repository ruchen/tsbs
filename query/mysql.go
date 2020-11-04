package query

import (
	"fmt"
	"sync"
)

// Encodes a request. This will be serialized for use
// by the tsbs_run_queries_mysql program.
type MysqlRequest struct {
	HumanLabel       []byte
	HumanDescription []byte

	Table []byte // e.g. "cpu"
	SqlQuery   []byte
	id         uint64
}

// MysqlPool is a sync.Pool of Mysql Query types
var MysqlPool = sync.Pool{
	New: func() interface{} {
		return &MysqlRequest{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
			Table:            make([]byte, 0, 1024),
			SqlQuery:         make([]byte, 0, 1024),
		}
	},
}

// NewMysql returns a new Mysql Query instance
func NewMysqlRequest() *MysqlRequest {
	return MysqlPool.Get().(*MysqlRequest)
}

// GetID returns the ID of this Query
func (q *MysqlRequest) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *MysqlRequest) SetID(n uint64) {
	q.id = n
}

// String produces a debug-ready description of a Query.
func (q *MysqlRequest) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, Table: %s, Query: %s", q.HumanLabel, q.HumanDescription, q.Table, q.SqlQuery)
}

// HumanLabelName returns the human readable name of this Query
func (q *MysqlRequest) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *MysqlRequest) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *MysqlRequest) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0

	q.Table = q.Table[:0]
	q.SqlQuery = q.SqlQuery[:0]

	MysqlPool.Put(q)
}
