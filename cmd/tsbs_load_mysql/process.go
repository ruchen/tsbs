package main 
import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/timescale/tsbs/load"
)

const (
	numExtraCols = 2 // one for tags_id, one for additional_tags
)

type syncCSI struct {
	m        map[string]int64
	mutex    *sync.RWMutex
	nextID   int64
	pcacheMux *sync.Mutex
	pcache    map[string]string
}

func newSyncCSI() *syncCSI {
	return &syncCSI{
		m:       make(map[string]int64),
		mutex:   &sync.RWMutex{},
		nextID: 1,
		pcacheMux: &sync.Mutex{},
		pcache:    make(map[string]string),
	}
}

var globalSyncCSI = newSyncCSI()

func subsystemTagsToJSON(tags []string) string {
	// This assumes "=" is only a divider and not in the RHS
	return fmt.Sprintf("{%s}", strings.ReplaceAll(strings.Join(tags, ","), "=", ":"))
}

func insertTags(db *sql.DB, tagRows [][]string) {
	cols := tagColsID
	values := make([]string, 0)
	commonTagsLen := len(tagColsID)

	for _, val := range tagRows {
		sqlValues := convertValsToSQLBasedOnType(val[:commonTagsLen], tagColumnTypesID[:commonTagsLen])
		row := fmt.Sprintf("(%s)", strings.Join(sqlValues, ","))
		values = append(values, row)
	}
	tx := MustBegin(db)
	res, err := tx.Exec(fmt.Sprintf(`INSERT INTO tags(%s) VALUES %s`, strings.Join(cols, ","), strings.Join(values, ",")))
	if err != nil {
		panic(err)
	}

	nr, err := res.RowsAffected()
	if err != nil {
		panic(err)
	}
	if int(nr) != len(tagRows) {
		panic(fmt.Sprintf("inserted %v tags, expected %v", nr, len(tagRows)))
	}

	err = tx.Commit()
	if err != nil {
		panic(err)
	}
}

// splitTagsAndMetrics takes an array of insertData (sharded by table) and
// divides the tags from data into appropriate slices that can then be used in
// SQL queries to insert into their respective tables. Additionally, it also
// returns the number of metrics (i.e., non-tag fields) for the data processed.
func splitTagsAndMetrics(rows []*insertData, dataCols int) ([][]string, [][]interface{}, uint64) {
	tagRows := make([][]string, 0, len(rows))
	dataRows := make([][]interface{}, 0, len(rows))
	numMetrics := uint64(0)
	commonTagsLen := len(tableCols[tagsKey])

	for _, data := range rows {
		// Split the tags into individual common tags and an extra bit leftover
		// for non-common tags that need to be added separately. For each of
		// the common tags, remove everything before = in the form <label>=<val>
		// since we won't need it.
		tags := strings.SplitN(data.tags, ",", commonTagsLen+1)
		for i := 0; i < commonTagsLen; i++ {
			tags[i] = strings.Split(tags[i], "=")[1]
		}

		var json interface{}
		if len(tags) > commonTagsLen {
			json = subsystemTagsToJSON(strings.Split(tags[commonTagsLen], ","))
		}

		metrics := strings.Split(data.fields, ",")
		numMetrics += uint64(len(metrics) - 1) // 1 field is timestamp

		timeInt, err := strconv.ParseInt(metrics[0], 10, 64)
		if err != nil {
			panic(err)
		}
		ts := time.Unix(0, timeInt)

		// use nil at 2nd position as placeholder for tagKey
		r := make([]interface{}, 3, dataCols)
		r[0], r[1], r[2] = ts, nil, json
		if inTableTag {
			r = append(r, tags[0])
		}
		for _, v := range metrics[1:] {
			if v == "" {
				r = append(r, nil)
				continue
			}

			num, err := strconv.ParseFloat(v, 64)
			if err != nil {
				panic(err)
			}

			r = append(r, num)
		}

		dataRows = append(dataRows, r)
		tagRows = append(tagRows, tags[:commonTagsLen])
	}

	return tagRows, dataRows, numMetrics
}

func makeInsert(table string, colNames []string, nRows int) string {
	nCols := len(colNames)
	colsS := fmt.Sprintf("(%s?)", strings.Repeat("?,", nCols-1))
	colsSComma := colsS + ","

	return fmt.Sprintf("INSERT INTO %s(%s) VALUES %s%s", table, strings.Join(colNames, ","), strings.Repeat(colsSComma, nRows-1), colsS)
}

func flatten(arr2d [][]interface{}) []interface{} {
	totalLen := 0
	for _, v := range arr2d {
		totalLen += len(v)
	}
	res := make([]interface{}, 0, totalLen)

	for _, v := range arr2d {
		res = append(res, v...)
	}

	if len(res) != totalLen {
		log.Fatalf("flatten %v != %v\n", len(res), totalLen)
	}
	return res
}

func (p *processor) lookupTags(tagRows [][]string) [][]string {
	// Determine which tags rows must be inserted. This updates the cached tags information
	// prior to inserting new rows and for that to work the tags.id column must be set in
	// the insert statement rather than relying on auto increment.

	maybeNewTags := make([][]string, 0, 32)
	newTags := [][]string(nil)

	// To reduce the sync bottleneck, first check with a read lock for missing entries.
	p.csi.mutex.RLock()
	for _, cols := range tagRows {
		if _, ok := p.csi.m[cols[0]]; !ok {
			maybeNewTags = append(maybeNewTags, cols)
		}
	}
	p.csi.mutex.RUnlock()

	if len(maybeNewTags) > 0 {
		// Get an exclusive lock, check and possibly add entries
		newTags = make([][]string, 0, len(maybeNewTags))

		p.csi.mutex.Lock()
		for _, cols := range maybeNewTags {
			if _, ok := p.csi.m[cols[0]]; !ok {
				p.csi.m[cols[0]] = p.csi.nextID
				cols = append(cols, strconv.FormatInt(p.csi.nextID, 10))
				p.csi.nextID++
				newTags = append(newTags, cols)
			}
		}
		p.csi.mutex.Unlock()
	}
	return newTags
}

func (p *processor) lookupPreparedStatement(table string, cols []string, dataRowsLen int) string {
	var ok bool
	var pstmt string

	key := fmt.Sprintf("%s.%d", table, dataRowsLen)

	p.csi.pcacheMux.Lock()
	if pstmt, ok = p.csi.pcache[key]; !ok {
		pstmt = makeInsert(table, cols, dataRowsLen)
		p.csi.pcache[key] = pstmt
		fmt.Printf("lookup pstmt added %v\n", key)
	}
	p.csi.pcacheMux.Unlock()
	return pstmt
}

func (p *processor) processCSI(table string, rows []*insertData) uint64 {
	colLen := len(tableCols[table]) + numExtraCols
	if inTableTag {
		colLen++
	}
	tagRows, dataRows, numMetrics := splitTagsAndMetrics(rows, colLen)

	newTags := p.lookupTags(tagRows)
	if len(newTags) > 0 {
		insertTags(p.db, newTags)
	}

	p.csi.mutex.RLock()
	for i := range dataRows {
		tagKey := tagRows[i][0]
		dataRows[i][1] = p.csi.m[tagKey]
	}
	p.csi.mutex.RUnlock()

	cols := make([]string, 0, colLen)
	cols = append(cols, "time", "tags_id", "additional_tags")
	if inTableTag {
		cols = append(cols, tableCols[tagsKey][0])
	}
	cols = append(cols, tableCols[table]...)

	pstmt := p.lookupPreparedStatement(table, cols, len(dataRows))

	tx := MustBegin(p.db)

	stmt, err := tx.Prepare(pstmt)
	if err != nil {
		panic(err)
	}

	flatRows := flatten(dataRows)
	_, err = stmt.Exec(flatRows...)
	if err != nil {
		panic(err)
	}

	err = stmt.Close()
	if err != nil {
		panic(err)
	}

	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	return numMetrics
}

type processor struct {
	db      *sql.DB
	csi     *syncCSI
}

func (p *processor) Init(workerNum int, doLoad bool) {
	if doLoad {
		p.db = MustConnect(getConnectString(true))
		// if hashWorkers { p.csi = newSyncCSI() }
		p.csi = globalSyncCSI
	}
}

func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.db.Close()
	}
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batches := b.(*tableArr)
	rowCnt := 0
	metricCnt := uint64(0)
	for table, rows := range batches.m {
		rowCnt += len(rows)
		if doLoad {
			start := time.Now()
			metricCnt += p.processCSI(table, rows)

			if logBatches {
				now := time.Now()
				took := now.Sub(start)
				batchSize := len(rows)
				fmt.Printf("BATCH: batchsize %d row rate %f/sec (took %v)\n", batchSize, float64(batchSize)/float64(took.Seconds()), took)
			}
		}
	}
	batches.m = map[string][]*insertData{}
	batches.cnt = 0
	return metricCnt, uint64(rowCnt)
}
func convertValsToSQLBasedOnType(values []string, types []string) []string {
	return convertValsToBasedOnType(values, types, "'", "NULL")
}

func convertValsToJSONBasedOnType(values []string, types []string) []string {
	return convertValsToBasedOnType(values, types, `"`, "null")
}

func convertValsToBasedOnType(values []string, types []string, quotemark string, null string) []string {
	sqlVals := make([]string, len(values))
	for i, val := range values {
		if val == "" {
			sqlVals[i] = null
			continue
		}
		switch types[i] {
		case "string":
			sqlVals[i] = quotemark + val + quotemark
		default:
			sqlVals[i] = val
		}
	}

	return sqlVals
}
