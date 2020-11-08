package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"testing"
)

func TestDBCreatorReadDataHeader(t *testing.T) {
	cases := []struct {
		desc         string
		input        string
		wantTags     string
		wantCols     []string
		wantBuffered int
		shouldFatal  bool
	}{
		{
			desc:         "min case: exactly three lines",
			input:        "tags,tag1,tag2\ncols,col1,col2\n\n",
			wantTags:     "tags,tag1,tag2",
			wantCols:     []string{"cols,col1,col2"},
			wantBuffered: 0,
		},
		{
			desc:         "min case: more than the header 3 lines",
			input:        "tags,tag1,tag2\ncols,col1,col2\n\nrow1\nrow2\n",
			wantTags:     "tags,tag1,tag2",
			wantCols:     []string{"cols,col1,col2"},
			wantBuffered: len([]byte("row1\nrow2\n")),
		},
		{
			desc:         "multiple tables: more than 3 lines for header",
			input:        "tags,tag1,tag2\ncols,col1,col2\ncols2,col21,col22\n\n",
			wantTags:     "tags,tag1,tag2",
			wantCols:     []string{"cols,col1,col2", "cols2,col21,col22"},
			wantBuffered: 0,
		},
		{
			desc:         "multiple tables: more than 3 lines for header w/ extra",
			input:        "tags,tag1,tag2\ncols,col1,col2\ncols2,col21,col22\n\nrow1\nrow2\n",
			wantTags:     "tags,tag1,tag2",
			wantCols:     []string{"cols,col1,col2", "cols2,col21,col22"},
			wantBuffered: len([]byte("row1\nrow2\n")),
		},
		{
			desc:        "too few lines",
			input:       "tags\ncols\n",
			shouldFatal: true,
		},
		{
			desc:        "no line ender",
			input:       "tags",
			shouldFatal: true,
		},
	}

	for _, c := range cases {
		dbc := &dbCreator{}
		br := bufio.NewReader(bytes.NewReader([]byte(c.input)))
		if c.shouldFatal {
			isCalled := false
			fatal = func(fmt string, args ...interface{}) {
				isCalled = true
				log.Printf(fmt, args...)
			}
			dbc.readDataHeader(br)
			if !isCalled {
				t.Errorf("%s: did not call fatal when it should", c.desc)
			}
		} else {
			dbc.readDataHeader(br)
			if dbc.tags != c.wantTags {
				t.Errorf("%s: incorrect tags: got\n%s\nwant\n%s", c.desc, dbc.tags, c.wantTags)
			}
			if len(dbc.cols) != len(c.wantCols) {
				t.Errorf("%s: incorrect cols len: got %d want %d", c.desc, len(dbc.cols), len(c.wantCols))
			}
			for i := range dbc.cols {
				if got := dbc.cols[i]; got != c.wantCols[i] {
					t.Errorf("%s: cols row %d incorrect: got\n%s\nwant\n%s\n", c.desc, i, got, c.wantCols[i])
				}
			}
			if br.Buffered() != c.wantBuffered {
				t.Errorf("%s: incorrect amt buffered: got\n%d\nwant\n%d", c.desc, br.Buffered(), c.wantBuffered)
			}
		}
	}
}

func TestDBCreatorGetCreateIndexOnFieldSQL(t *testing.T) {
	table := "htable"
	field := "foo"
	valueTime := "CREATE INDEX x_foo_time ON htable (foo, `time` DESC)"
	timeValue := "CREATE INDEX x_time_foo ON htable (`time` DESC, foo)"
	cases := []struct {
		desc        string
		idxType     string
		want        []string
		shouldFatal bool
	}{
		{
			desc:    "no indexes",
			idxType: "",
			want:    []string{},
		},
		{
			desc:    "single TIME-VALUE index",
			idxType: timeValueIdx,
			want:    []string{timeValue},
		},
		{
			desc:    "single VALUE-TIME index",
			idxType: valueTimeIdx,
			want:    []string{valueTime},
		},
		{
			desc:    "two indexes",
			idxType: timeValueIdx + "," + valueTimeIdx,
			want:    []string{timeValue, valueTime},
		},
		{
			desc:        "bad idxType",
			idxType:     "baz",
			shouldFatal: true,
		},
	}

	for _, c := range cases {
		dbc := &dbCreator{}
		if c.shouldFatal {
			isCalled := false
			fatal = func(fmt string, args ...interface{}) {
				isCalled = true
				log.Printf(fmt, args...)
			}
			dbc.getCreateIndexOnFieldCmds(table, field, c.idxType)
			if !isCalled {
				t.Errorf("%s: did not call fatal when it should", c.desc)
			}
		} else {
			cmds := dbc.getCreateIndexOnFieldCmds(table, field, c.idxType)
			if len(cmds) != len(c.want) {
				t.Errorf("%s: incorrect cmds length: got %d want %d", c.desc, len(cmds), len(c.want))
			}
			for i, cmd := range cmds {
				if cmd != c.want[i] {
					t.Errorf("%s: incorrect cmd at idx %d: got %s want %s", c.desc, i, cmd, c.want[i])
				}
			}
		}
	}
}

func TestDBCreatorGetFieldAndIndexDefinitions(t *testing.T) {
	cases := []struct {
		desc            string
		columns         []string
		fieldIndexCount int
		wantFieldDefs   []string
		wantIndexDefs   []string
	}{
		{
			desc:            "no field indexes, in table tag",
			columns:         []string{"cpu", "usage_user", "usage_system", "usage_idle", "usage_nice"},
			fieldIndexCount: 0,
			wantFieldDefs:   []string{"usage_user DOUBLE", "usage_system DOUBLE", "usage_idle DOUBLE", "usage_nice DOUBLE"},
			wantIndexDefs:   []string{},
		},
		{
			desc:            "one field index",
			columns:         []string{"cpu", "usage_user", "usage_system", "usage_idle", "usage_nice"},
			fieldIndexCount: 1,
			wantFieldDefs:   []string{"usage_user DOUBLE", "usage_system DOUBLE", "usage_idle DOUBLE", "usage_nice DOUBLE"},
			wantIndexDefs:   []string{"CREATE INDEX x_usage_user_time ON cpu (usage_user, `time` DESC)"},
		},
		{
			desc:            "two field indexes",
			columns:         []string{"cpu", "usage_user", "usage_system", "usage_idle", "usage_nice"},
			fieldIndexCount: 2,
			wantFieldDefs:   []string{"usage_user DOUBLE", "usage_system DOUBLE", "usage_idle DOUBLE", "usage_nice DOUBLE"},
			wantIndexDefs:   []string{"CREATE INDEX x_usage_user_time ON cpu (usage_user, `time` DESC)", "CREATE INDEX x_usage_system_time ON cpu (usage_system, `time` DESC)"},
		},
	}

	for _, c := range cases {
		// Initialize global cache
		tableCols[tagsKey] = []string{}
		tableCols[tagsKey] = append(tableCols[tagsKey], "hostname")
		dbc := &dbCreator{}
		fieldIndexCount = c.fieldIndexCount
		fieldDefs, indexDefs := dbc.getFieldAndIndexDefinitions(c.columns)
		for i, fieldDef := range fieldDefs {
			if fieldDef != c.wantFieldDefs[i] {
				t.Errorf("%s: incorrect fieldDef at idx %d: got %s want %s", c.desc, i, fieldDef, c.wantFieldDefs[i])
			}
		}
		for i, indexDef := range indexDefs {
			if indexDef != c.wantIndexDefs[i] {
				t.Errorf("%s: incorrect indexDef at idx %d: got %s want %s", c.desc, i, indexDef, c.wantIndexDefs[i])
			}
		}
	}
}

func TestExtractTagNamesAndTypes(t *testing.T) {
	names, types := extractTagNamesAndTypes([]string{"tag1 type1", "tag2 type2"})
	if names[0] != "`tag1`" || names[1] != "`tag2`" {
		t.Errorf("expected tag names tag1 and tag2, got: %v", names)
	}
	if types[0] != "type1" || types[1] != "type2" {
		t.Errorf("expected tag types type1 and type2, got: %v", types)

	}
}
func TestGenerateTagsTableQuery(t *testing.T) {
	testCases := []struct {
		in  []string
		inT []string
		out string
	}{
		{
			in:  []string{"tag1"},
			inT: []string{"string"},
			out: "CREATE TABLE tags(id bigint PRIMARY KEY, tag1 VARCHAR(256))",
		}, {
			in:  []string{"tag1", "tag2", "tag3", "tag4"},
			inT: []string{"int32", "int64", "float32", "float64"},
			out: "CREATE TABLE tags(id bigint PRIMARY KEY, tag1 INTEGER, tag2 BIGINT," +
				" tag3 FLOAT, tag4 DOUBLE)",
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Generate tags table for: %s", tc.in), func(t *testing.T) {
			res := generateTagsTableQuery(tc.in, tc.inT)
			if res != tc.out {
				t.Errorf("tags table not properly created\n expected: %s\n got: %s\n", tc.out, res)
			}
		})
	}
}

func TestGenerateTagsTableQueryPanicOnWrongType(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("did not panic when should")
		}
	}()

	generateTagsTableQuery([]string{"tag"}, []string{"uint32"})

	t.Fatalf("test should have stopped at this point")
}
