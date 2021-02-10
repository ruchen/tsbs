package main

import (
	"context"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
	"github.com/timescale/tsbs/load"
)

// naiveBenchmark allows you to run a benchmark using the naive, one document per
// event Mongo approach
type naiveBenchmark struct {
	mongoBenchmark
}

func newNaiveBenchmark(l *load.BenchmarkRunner) *naiveBenchmark {
	return &naiveBenchmark{mongoBenchmark{l, &dbCreator{}}}
}

func (b *naiveBenchmark) GetProcessor() load.Processor {
	return &naiveProcessor{dbc: b.dbc}
}

func (b *naiveBenchmark) GetPointIndexer(_ uint) load.PointIndexer {
	return &load.ConstantIndexer{}
}

type singlePoint map[string]interface{}

var spPool = &sync.Pool{New: func() interface{} { return &singlePoint{} }}

type naiveProcessor struct {
	dbc        *dbCreator
	collection *mongo.Collection

	pvs []interface{}
}

func (p *naiveProcessor) Init(workerNUm int, doLoad bool) {
	if doLoad {
		p.collection = p.dbc.client.Database(loader.DatabaseName()).Collection(collectionName)
	}
	p.pvs = []interface{}{}
}

// ProcessBatch creates a new document for each incoming event for a simpler
// approach to storing the data. This is _NOT_ the default since the aggregation method
// is recommended by Mongo and other blogs
func (p *naiveProcessor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch).arr
	if cap(p.pvs) < len(batch) {
		p.pvs = make([]interface{}, len(batch))
	}
	p.pvs = p.pvs[:len(batch)]
	var metricCnt uint64
	for i, event := range batch {
		x := spPool.Get().(*singlePoint)

		(*x)["measurement"] = string(event.MeasurementName())
		(*x)[timestampField] = time.Unix(0, event.Timestamp())
		(*x)["tags"] = map[string]string{}
		f := &serialize.MongoReading{}
		for j := 0; j < event.FieldsLength(); j++ {
			event.Fields(f, j)
			(*x)[string(f.Key())] = f.Value()
		}
		t := &serialize.MongoTag{}
		for j := 0; j < event.TagsLength(); j++ {
			event.Tags(t, j)
			(*x)["tags"].(map[string]string)[string(t.Key())] = string(t.Value())
		}
		p.pvs[i] = x
		metricCnt += uint64(event.FieldsLength())
	}

	if doLoad {
		opts := options.InsertMany().SetOrdered(orderedInserts)
		_, err := p.collection.InsertMany(context.Background(), p.pvs, opts)
		if err != nil {
			log.Fatalf("Bulk insert docs err: %s\n", err.Error())
		}
	}
	for _, p := range p.pvs {
		spPool.Put(p)
	}

	return metricCnt, 0
}
