// tsbs_load_timescaledb loads a TimescaleDB instance with data from stdin.
//
// If the database exists beforehand, it will be *DROPPED*.
package main

import (
	"bufio"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
)

const (
	timeValueIdx = "TIME-VALUE"
	valueTimeIdx = "VALUE-TIME"
)

// Program option vars:
var (
	host            string
	user            string
	pass            string
	port            string

	logBatches    bool
	hashWorkers   bool

	numberPartitions int
	chunkTime        time.Duration

	timeHostIndex      bool
	hostTimeIndex      bool
	timeHostIndexPK    bool
	hostTimeIndexPK    bool
	fieldIndex         string
	fieldIndexCount    int

	profileFile          string
	replicationStatsFile string

	createMetricsTable bool
	analyze            bool
	tagColumnTypes     []string
	tagColumnTypesID   []string
)

type insertData struct {
	tags   string
	fields string
}

// Global vars
var loader *load.BenchmarkRunner

// allows for testing
var fatal = log.Fatalf

// Parse args:
func init() {
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("host", "localhost", "Hostname of MySQL instance")
	pflag.String("port", "3306", "Which port to connect to on the database host")
	pflag.String("user", "root", "User to connect to MySQL as")
	pflag.String("pass", "", "Password for user connecting to MySQL")

	pflag.Bool("log-batches", false, "Whether to time individual batches.")

	// TODO - This flag could potentially be done as a string/enum with other options besides no-hash, round-robin, etc
	pflag.Bool("hash-workers", false, "Whether to consistently hash insert data to the same workers (i.e., the data for a particular host always goes to the same worker)")

	pflag.Int("partitions", 1, "Number of partitions")
	pflag.Duration("chunk-time", 12*time.Hour, "Duration that each chunk should represent, e.g., 12h")

	pflag.Bool("time-host-index", true, "Whether to build an index on (time,host)")
	pflag.Bool("host-time-index", true, "Whether to build an index on (host,time)")

	pflag.Bool("time-host-index-pk", true, "Whether (time,host) index is the PK")
	pflag.Bool("host-time-index-pk", false, "Whether (host,time) index is the PK")

	pflag.String("field-index", valueTimeIdx, "index types for tags (comma delimited)")
	pflag.Int("field-index-count", 0, "Number of indexed fields (-1 for all)")

	pflag.String("write-profile", "", "File to output CPU/memory profile to")
	pflag.String("write-replication-stats", "", "File to output replication stats to")
	pflag.Bool("create-metrics-table", true, "Drops existing and creates new metrics table")
	pflag.Bool("analyze", true, "Analyze each table after the load")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	host = viper.GetString("host")
	port = viper.GetString("port")
	user = viper.GetString("user")
	pass = viper.GetString("pass")
	logBatches = viper.GetBool("log-batches")

	hashWorkers = viper.GetBool("hash-workers")

	numberPartitions = viper.GetInt("partitions")
	chunkTime = viper.GetDuration("chunk-time")

	timeHostIndex = viper.GetBool("time-host-index")
	hostTimeIndex = viper.GetBool("host-time-index")
	timeHostIndexPK = viper.GetBool("time-host-index-pk")
	hostTimeIndexPK = viper.GetBool("host-time-index-pk")

	if timeHostIndexPK && !timeHostIndex {
		panic("time-host-index must be true if time-host-index-pk is true")
	}
	if hostTimeIndexPK && !hostTimeIndex {
		panic("host-time-index must be true if host-time-index-pk is true")
	}
	if timeHostIndexPK && hostTimeIndexPK {
		panic("At most one of time-host-index-pk and host-time-index-pk can be True")
	}

	fieldIndex = viper.GetString("field-index")
	fieldIndexCount = viper.GetInt("field-index-count")

	profileFile = viper.GetString("write-profile")
	replicationStatsFile = viper.GetString("write-replication-stats")
	createMetricsTable = viper.GetBool("create-metrics-table")
	analyze = viper.GetBool("analyze")

	loader = load.GetBenchmarkRunner(config)
}

type benchmark struct{}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{scanner: bufio.NewScanner(br)}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) load.PointIndexer {
	if hashWorkers {
		return &hostnameIndexer{partitions: maxPartitions}
	}
	return &load.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return &dbCreator{
		br:      loader.GetBufferedReader(),
		// connStr: getConnectString(),
		tables:  make([]string, 0, 10),
	}
}

func main() {
	// If specified, generate a performance profile
	if len(profileFile) > 0 {
		go profileCPUAndMem(profileFile)
	}

	var replicationStatsWaitGroup sync.WaitGroup
	// if len(replicationStatsFile) > 0 {
	//	go OutputReplicationStats(getConnectString(), replicationStatsFile, &replicationStatsWaitGroup)
	// }

	if hashWorkers {
		loader.RunBenchmark(&benchmark{}, load.WorkerPerQueue)
	} else {
		loader.RunBenchmark(&benchmark{}, load.SingleQueue)
	}

	if len(replicationStatsFile) > 0 {
		replicationStatsWaitGroup.Wait()
	}
}

func getConnectString(withDB bool) string {
	var cs string // "root:pw@tcp(127.0.0.1:3306)/test")
	// TODO should db name be used?
	if withDB {
		if len(pass) > 0 {
			cs = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?loc=Local", user, pass, host, port, loader.DatabaseName())
		} else {
			cs = fmt.Sprintf("%s@tcp(%s:%s)/%s?loc=Local", user, host, port, loader.DatabaseName())
		}
	} else {
		if len(pass) > 0 {
			cs = fmt.Sprintf("%s:%s@tcp(%s:%s)/?loc=Local", user, pass, host, port)
		} else {
			cs = fmt.Sprintf("%s@tcp(%s:%s)/?loc=Local", user, host, port)
		}
	}

	// fmt.Printf("getConnectString: %v\n", cs)
	return cs
}
