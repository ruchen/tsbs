// bulk_load_mongo loads a Mongo daemon with data from stdin.
//
// Any existing collections in the database will be removed.
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
)

const (
	collectionName     = "point_data"
	aggDocID           = "doc_id"
	aggDateFmt         = "20060102_15" // see Go docs for how we arrive at this time format
	aggKeyID           = "key_id"
	aggInsertBatchSize = 500 // found via trial-and-error
	timestampField     = "time"
)

// Program option vars:
var (
	daemonURL            string
	documentPer          bool
	writeTimeout         time.Duration
	timeseriesCollection bool
	retryableWrites      bool
	orderedInserts       bool
	randomFieldOrder     bool
	timeseriesCollectionSharded bool
	numInitChunks        uint
	shardKeySpec         string
	loadBalancerOn       bool
)

// Global vars
var (
	loader *load.BenchmarkRunner
)

// Parse args:
func init() {
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("url", "mongodb://localhost:27017", "Mongo URL.")
	pflag.Duration("write-timeout", 10*time.Second, "Write timeout.")
	pflag.Bool("document-per-event", false, "Whether to use one document per event or aggregate by hour")
	pflag.Bool("timeseries-collection", false, "Whether to use a time-series collection")
	pflag.Bool("retryable-writes", true, "Whether to use retryable writes")
	pflag.Bool("ordered-inserts", true, "Whether to use ordered inserts")
	pflag.Bool("random-field-order", true, "Whether to use random field order")
	pflag.Bool("timeseries-collection-sharded", false, "Whether to shard a time-series collection")
	pflag.Uint("number-initial-chunks", 0, "number of initial chunks to create and distribute of an empty collection; if 0 then do not specifiy it")
	pflag.String("shard-key-spec", "{time:1}", "shard key spec")
	pflag.String("load-balancer-on", "true", "whether to keep load balancer on")
	
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	daemonURL = viper.GetString("url")
	writeTimeout = viper.GetDuration("write-timeout")
	documentPer = viper.GetBool("document-per-event")
	timeseriesCollection = viper.GetBool("timeseries-collection")
	retryableWrites = viper.GetBool("retryable-writes")
	orderedInserts = viper.GetBool("ordered-inserts")
	randomFieldOrder = viper.GetBool("random-field-order")
	timeseriesCollectionSharded = viper.GetBool("timeseries-collection-sharded")
	numInitChunks = viper.GetUint("number-initial-chunks")
	shardKeySpec = viper.GetString("shard-key-spec")
	loadBalancerOn = viper.GetBool("load-balancer-on")

	if !documentPer && timeseriesCollection {
		log.Fatal("Must set document-per-event=true in order to use timeseries-collection=true")
	}
	if !timeseriesCollection && timeseriesCollectionSharded {
		log.Fatal("Must set timeseries-collection=true in order to use timeseries-collection-sharded=true")
	}

	loader = load.GetBenchmarkRunner(config)
}

func main() {
	var benchmark load.Benchmark
	var workQueues uint
	if documentPer {
		benchmark = newNaiveBenchmark(loader)
		workQueues = load.SingleQueue
	} else {
		benchmark = newAggBenchmark(loader)
		workQueues = load.WorkerPerQueue
	}

	loader.RunBenchmark(benchmark, workQueues)
}
