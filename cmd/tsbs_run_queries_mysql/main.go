// tsbs_run_queries_mysql speed tests MySQL using requests from stdin or file
//
// It reads encoded Query objects from stdin or file, and makes concurrent requests
// to the provided MySQL endpoint.
// This program has no knowledge of the internals of the endpoint.
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/query"
)

// Program option vars:
var (
	mysqlConnect  string
	hostList      []string
	user          string
	pass          string
	port          string
	showExplain   bool
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("hosts", "localhost", "Comma separated list of MySQL hosts (pass multiple values for sharding reads on a multi-node setup)")
	pflag.String("user", "root", "User to connect to MySQL as")
	pflag.String("pass", "", "Password for the user connecting to MySQL (leave blank if not password protected)")
	pflag.String("port", "3306", "Which port to connect to on the database host")

	pflag.Bool("show-explain", false, "Print out the EXPLAIN output for sample query")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	hosts := viper.GetString("hosts")
	user = viper.GetString("user")
	pass = viper.GetString("pass")
	port = viper.GetString("port")
	showExplain = viper.GetBool("show-explain")

	runner = query.NewBenchmarkRunner(config)

	if showExplain {
		runner.SetLimit(1)
	}

	// Parse comma separated string of hosts and put in a slice (for multi-node setups)
	for _, host := range strings.Split(hosts, ",") {
		hostList = append(hostList, host)
	}
}

func main() {
	runner.Run(&query.MysqlPool, newProcessor)
}

func getConnectString(workerNumber int, dbName string) string {
        var cs string // "root:pw@tcp(127.0.0.1:3306)/test")

	// Round robin the host/worker assignment by assigning a host based on workerNumber % totalNumberOfHosts
	host := hostList[workerNumber%len(hostList)]

	if len(pass) > 0 {
		cs = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?loc=Local", user, pass, host, port, dbName)
	} else {
		cs = fmt.Sprintf("%s@tcp(%s:%s)/%s?loc=Local", user, host, port, dbName)
	}

        // fmt.Printf("getConnectString: %v\n", cs)
        return cs
}

// prettyPrintResponse prints a Query and its response in JSON format with two
// keys: 'query' which has a value of the SQL used to generate the second key
// 'results' which is an array of each row in the return set.
func prettyPrintResponse(rows *sql.Rows, q *query.MysqlRequest) {
	resp := make(map[string]interface{})
	resp["query"] = string(q.SqlQuery)
	resp["results"] = mapRows(rows)

	line, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(line) + "\n")
}

func mapRows(r *sql.Rows) []map[string]interface{} {
	rows := []map[string]interface{}{}
	cols, _ := r.Columns()
	for r.Next() {
		row := make(map[string]interface{})
		values := make([]interface{}, len(cols))
		for i := range values {
			values[i] = new(interface{})
		}

		err := r.Scan(values...)
		if err != nil {
			panic(errors.Wrap(err, "error while reading values"))
		}

		for i, column := range cols {
			row[column] = *values[i].(*interface{})
		}
		rows = append(rows, row)
	}
	return rows
}

type queryExecutorOptions struct {
	showExplain   bool
	debug         bool
	printResponse bool
}

type processor struct {
	db   *sql.DB
	opts *queryExecutorOptions
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	db, err := sql.Open("mysql", getConnectString(workerNumber, runner.DatabaseName()))
	if err != nil {
		panic(err)
	}
	p.db = db
	p.opts = &queryExecutorOptions{
		showExplain:   showExplain,
		debug:         runner.DebugLevel() > 0,
		printResponse: runner.DoPrintResponses(),
	}
}

func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {
	// No need to run again for EXPLAIN
	if isWarm && p.opts.showExplain {
		return nil, nil
	}
	tq := q.(*query.MysqlRequest)

	start := time.Now()
	qry := string(tq.SqlQuery)
	if showExplain {
		qry = "EXPLAIN format=tree " + qry
	}
	rows, err := p.db.Query(qry)
	if err != nil {
		return nil, err
	}

	if p.opts.debug {
		fmt.Println(qry)
	}
	if showExplain {
		text := ""
		for rows.Next() {
			var s string
			if err2 := rows.Scan(&s); err2 != nil {
				panic(err2)
			}
			text += s + "\n"
		}
		fmt.Printf("%s\n\n%s\n-----\n\n", qry, text)
	} else if p.opts.printResponse {
		prettyPrintResponse(rows, tq)
	}
	// Fetching all the rows to confirm that the query is fully completed.
	for rows.Next() {
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, err
}
