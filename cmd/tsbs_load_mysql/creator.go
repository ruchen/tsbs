package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

const tagsKey = "tags"

var tableCols = make(map[string][]string)
var tagColsID = make([]string, 0)

type dbCreator struct {
	br      *bufio.Reader
	tags    string
	cols    []string
	tables	[]string
	// connStr string
}

func (d *dbCreator) Init() {
	d.readDataHeader(d.br)
}

func (d *dbCreator) readDataHeader(br *bufio.Reader) {
	// First N lines are header, with the first line containing the tags
	// and their names, the second through N-1 line containing the column
	// names, and last line being blank to separate from the data
	i := 0
	for {
		var err error
		var line string
		if i == 0 {
			d.tags, err = br.ReadString('\n')
			if err != nil {
				fatal("input has wrong header format: %v", err)
			}
			d.tags = strings.TrimSpace(d.tags)
		} else {
			line, err = br.ReadString('\n')
			if err != nil {
				fatal("input has wrong header format: %v", err)
			}
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				break
			}
			d.cols = append(d.cols, line)
		}
		i++
	}
}

// MustConnect connects or exits on errors
func MustConnect(connStr string) *sql.DB {
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		panic(err)
	}
	return db
}

// MustExec executes query or exits on error
func MustExec(db *sql.DB, query string, args ...interface{}) sql.Result {
	r, err := db.Exec(query, args...)
	if err != nil {
		log.Printf("Exec error: %s", query)
		panic(err)
	}
	return r
}

// MustQuery executes query or exits on error
func MustQuery(db *sql.DB, query string, args ...interface{}) *sql.Rows {
	r, err := db.Query(query, args...)
	if err != nil {
		panic(err)
	}
	return r
}

// MustBegin starts transaction or exits on error
func MustBegin(db *sql.DB) *sql.Tx {
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	return tx
}

func (d *dbCreator) DBExists(dbName string) bool {
	db := MustConnect(getConnectString(false))
	defer db.Close()
	r := MustQuery(db, "SELECT 1 from information_schema.schemata WHERE schema_name = ?", dbName)
	defer r.Close()
	return r.Next()
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	db := MustConnect(getConnectString(false))
	defer db.Close()
	MustExec(db, "DROP DATABASE IF EXISTS "+dbName)
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	db := MustConnect(getConnectString(false))
	MustExec(db, "CREATE DATABASE "+dbName)
	db.Close()
	return nil
}

func (d *dbCreator) Close() {
	if !analyze {
		return
	}

	db := MustConnect(getConnectString(true))
	for _, tname := range d.tables {
		fmt.Printf("Close: analyze %s\n", tname)
		MustExec(db, "ANALYZE TABLE "+tname)
	}
	db.Close()
}

func (d *dbCreator) PostCreateDB(dbName string) error {
	dbBench := MustConnect(getConnectString(true))
	defer dbBench.Close()

	tags := strings.Split(strings.TrimSpace(d.tags), ",")
	if tags[0] != tagsKey {
		return fmt.Errorf("input header in wrong format. got '%s', expected 'tags'", tags[0])
	}
	tagNames, tagTypes := extractTagNamesAndTypes(tags[1:])
	if createMetricsTable {
		createTagsTable(dbBench, tagNames, tagTypes)
		d.tables = append(d.tables, "tags")
	}
	// tableCols is a global map. Globally cache the available tags
	tableCols[tagsKey] = tagNames
	tagColsID = append(tagNames, "id")
	// tagTypes holds the type of each tag value (as strings from Go types (string, float32...))
	tagColumnTypes = tagTypes
	tagColumnTypesID = append(tagColumnTypes, "bigint")

	// Each table is defined in the dbCreator 'cols' list. The definition consists of a
	// comma separated list of the table name followed by its columns. Iterate over each
	// definition to update our global cache and create the requisite tables and indexes
	for _, tableDef := range d.cols {
		var columns []string
		for x,v := range strings.Split(strings.TrimSpace(tableDef), ",") {
			if x > 0 {
				v = fmt.Sprintf("`%s`", v)
			}
			columns = append(columns, v)
		}
		tableName := columns[0]
		// tableCols is a global map. Globally cache the available columns for the given table
		tableCols[tableName] = columns[1:]

		fieldDefs, indexDefs := d.getFieldAndIndexDefinitions(columns)
		if createMetricsTable {
			d.createTableAndIndexes(dbBench, tableName, fieldDefs, indexDefs)
		}
	}
	return nil
}

// getFieldAndIndexDefinitions iterates over a list of table columns, populating lists of
// definitions for each desired field and index. Returns separate lists of fieldDefs and indexDefs
func (d *dbCreator) getFieldAndIndexDefinitions(columns []string) ([]string, []string) {
	var fieldDefs []string
	var indexDefs []string
	var allCols []string

	tableName := columns[0]

	allCols = append(allCols, columns[1:]...)
	extraCols := 0 // set to 1 when hostname is kept in-table
	for idx, field := range allCols {
		if len(field) == 0 {
			continue
		}
		fieldType := "DOUBLE"
		idxType := fieldIndex

		fieldDefs = append(fieldDefs, fmt.Sprintf("%s %s", field, fieldType))
		// If the user specifies indexes on additional fields, add them to
		// our index definitions until we've reached the desired number of indexes
		if fieldIndexCount == -1 || idx < (fieldIndexCount+extraCols) {
			indexDefs = append(indexDefs, d.getCreateIndexOnFieldCmds(tableName, field, idxType)...)
		}
	}
	return fieldDefs, indexDefs
}

// createTableAndIndexes takes a list of field and index definitions for a given tableName and constructs
// the necessary table and index based on the user's settings
func (d *dbCreator) createTableAndIndexes(dbBench *sql.DB, tableName string, fieldDefs []string, indexDefs []string) {
	MustExec(dbBench, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	MustExec(dbBench, fmt.Sprintf("CREATE TABLE %s (`time` TIMESTAMP NOT NULL, tags_id BIGINT NOT NULL, hostname VARCHAR(256), additional_tags VARCHAR(256) DEFAULT NULL, %s)",
		tableName, strings.Join(fieldDefs, " NOT NULL,")))
	d.tables = append(d.tables, tableName)

	if hostTimeIndex {
		if hostTimeIndexPK {
			MustExec(dbBench, fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY(hostname, `time` DESC)", tableName))
		} else {
			MustExec(dbBench, fmt.Sprintf("CREATE INDEX six ON %s(hostname, `time` DESC)", tableName))
		}
	}

	if timeHostIndex {
		if timeHostIndexPK {
			MustExec(dbBench, fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY(`time` DESC, hostname)", tableName))
		} else {
			MustExec(dbBench, fmt.Sprintf("CREATE INDEX six ON %s(`time` DESC, hostname)", tableName))
		}
	}

	for _, indexDef := range indexDefs {
		MustExec(dbBench, indexDef)
	}

}

func (d *dbCreator) getCreateIndexOnFieldCmds(table, field, idxType string) []string {
	ret := []string{}
	for _, idx := range strings.Split(idxType, ",") {
		if idx == "" {
			continue
		}

		indexDef := ""
		var iname string
		if idx == timeValueIdx {
			indexDef = fmt.Sprintf("(`time` DESC, %s)", field)
			iname = fmt.Sprintf("x_time_%s", field)
		} else if idx == valueTimeIdx {
			indexDef = fmt.Sprintf("(%s, `time` DESC)", field)
			iname = fmt.Sprintf("x_%s_time", field)
		} else {
			fatal("Unknown index type %v", idx)
		}

		ret = append(ret, fmt.Sprintf("CREATE INDEX %s ON %s %s", iname, table, indexDef))
	}
	return ret
}

func createTagsTable(db *sql.DB, tagNames, tagTypes []string) {
	MustExec(db, "DROP TABLE IF EXISTS tags")

	MustExec(db, generateTagsTableQuery(tagNames, tagTypes))
	MustExec(db, fmt.Sprintf("CREATE UNIQUE INDEX uniq1 ON tags(%s)", strings.Join(tagNames, ",")))
	MustExec(db, fmt.Sprintf("CREATE INDEX xhost ON tags(%s)", tagNames[0]))
}

func generateTagsTableQuery(tagNames, tagTypes []string) string {
	tagColumnDefinitions := make([]string, len(tagNames))
	for i, tagName := range tagNames {
		pgType := serializedTypeToMyType(tagTypes[i])
		tagColumnDefinitions[i] = fmt.Sprintf("%s %s", tagName, pgType)
	}

	cols := strings.Join(tagColumnDefinitions, ", ")
	return fmt.Sprintf("CREATE TABLE tags(id bigint PRIMARY KEY, %s)", cols)
}

func extractTagNamesAndTypes(tags []string) ([]string, []string) {
	tagNames := make([]string, len(tags))
	tagTypes := make([]string, len(tags))
	for i, tagWithType := range tags {
		tagAndType := strings.Split(tagWithType, " ")
		if len(tagAndType) != 2 {
			panic("tag header has invalid format")
		}
		tagNames[i] = fmt.Sprintf("`%s`", tagAndType[0])
		tagTypes[i] = tagAndType[1]
	}

	return tagNames, tagTypes
}

func serializedTypeToMyType(serializedType string) string {
	switch serializedType {
	case "string":
		return "VARCHAR(256)"
	case "float32":
		return "FLOAT"
	case "float64":
		return "DOUBLE"
	case "int64":
		return "BIGINT"
	case "int32":
		return "INTEGER"
	default:
		panic(fmt.Sprintf("unrecognized type %s", serializedType))
	}
}
