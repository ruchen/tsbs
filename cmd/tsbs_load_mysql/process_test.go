package main

import (
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestSubsystemTagsToJSON(t *testing.T) {
	cases := []struct {
		desc string
		tags []string
		want string
	}{
		{
			desc: "empty tag list",
			tags: []string{},
			want: "{}",
		},
		{
			desc: "only one tag (no commas needed)",
			tags: []string{"foo=1"},
			want: "{foo:1}",
		},
		{
			desc: "two tags (need comma)",
			tags: []string{"foo=1", "bar=baz"},
			want: "{foo:1,bar:baz}",
		},
		{
			desc: "three tags",
			tags: []string{"foo=1", "bar=baz", "test=true"},
			want: "{foo:1,bar:baz,test:true}",
		},
	}

	for _, c := range cases {
		got := subsystemTagsToJSON(c.tags)
		if got != c.want {
			t.Errorf("%s: incorrect result: got %v want %v", c.desc, got, c.want)
		}
	}
}

func TestSplitTagsAndMetrics(t *testing.T) {
	numCols := 4
	tableCols[tagsKey] = []string{"hostname", "tag1", "tag2"}
	toTS := func(s string) string {
		timeInt, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			panic(err)
		}
		return time.Unix(0, timeInt).Format(time.RFC3339)
	}

	justTagsData := []*insertData{
		{
			tags:   "hostname=h1,tag1=foo,tag2=bar",
			fields: "100,1,5,42",
		},
		{
			tags:   "hostname=h2,tag1=foofoo,tag2=barbar",
			fields: "200,1,5,45",
		},
	}

	extraTagsData := []*insertData{
		{
			tags:   "hostname=h1,tag1=foo,tag2=bar,tag3=baz",
			fields: "100,1,5,42",
		},
		{
			tags:   "hostname=h2,tag1=foofoo,tag2=barbar,tag3=BAZ",
			fields: "200,1,5,45",
		},
	}

	cases := []struct {
		desc        string
		rows        []*insertData
		wantMetrics uint64
		wantTags    [][]string
		wantData    [][]interface{}
		shouldPanic bool
	}{
		{
			desc:        "just common, in table tag",
			rows:        justTagsData,
			wantMetrics: 6,
			wantTags:    [][]string{{"h1", "foo", "bar"}, {"h2", "foofoo", "barbar"}},
			wantData: [][]interface{}{
				[]interface{}{toTS("100"), nil, "h1", nil, 1.0, 5.0, 42.0},
				[]interface{}{toTS("200"), nil, "h2", nil, 1.0, 5.0, 45.0},
			},
		},
		{
			desc:        "extra tags",
			rows:        extraTagsData,
			wantMetrics: 6,
			wantTags:    [][]string{{"h1", "foo", "bar"}, {"h2", "foofoo", "barbar"}},
			wantData: [][]interface{}{
				[]interface{}{toTS("100"), nil, "h1", "{tag3:baz}", 1.0, 5.0, 42.0},
				[]interface{}{toTS("200"), nil, "h2", "{tag3:BAZ}", 1.0, 5.0, 45.0},
			},
		},
		{
			desc: "invalid timestamp",
			rows: []*insertData{
				{
					tags:   "tag1=foo,tag2=bar,tag3=baz",
					fields: "not_a_timestamp,1,5,42",
				},
			},
			shouldPanic: true,
		},
		{
			desc: "empty tag value",
			rows: []*insertData{
				{
					tags:   "tag1=,tag2=bar",
					fields: "100,1,5,42",
				},
			},
			wantTags: [][]string{{"", "bar"}},
			wantData: [][]interface{}{
				[]interface{}{toTS("100"), nil, nil, nil, 1.0, 5.0, 42.0},
			},
		},
		{
			desc: "empty extra tag value",
			rows: []*insertData{
				{
					tags:   "tag1=foo,tag2=bar,tag3=",
					fields: "100,1,5,42",
				},
			},
			wantTags: [][]string{{"foo", "bar"}},
			wantData: [][]interface{}{
				[]interface{}{toTS("100"), nil, nil, "{tag3:\"\"}", 1.0, 5.0, 42.0},
			},
		},
		{
			desc: "empty field value",
			rows: []*insertData{
				{
					tags:   "tag1=foo,tag2=bar",
					fields: "100,,5,42",
				},
			},
			wantTags: [][]string{{"foo", "bar"}},
			wantData: [][]interface{}{
				[]interface{}{toTS("100"), nil, nil, nil, nil, 5.0, 42.0},
			},
		},
	}

	for rx, c := range cases {
		if c.shouldPanic {
			defer func() {
				if re := recover(); re == nil {
					t.Errorf("%s: did not panic when should, row %d", c.desc, rx)
				}
			}()
			splitTagsAndMetrics(c.rows, numCols+numExtraCols)
		}

		gotTags, gotData, numMetrics := splitTagsAndMetrics(c.rows, numCols+numExtraCols)

		if numMetrics != c.wantMetrics {
			t.Errorf("%s: number of metrics incorrect: got %d want %d, row %d", c.desc, numMetrics, c.wantMetrics, rx)
		}

		if got := len(gotTags); got != len(c.wantTags) {
			t.Errorf("%s: tags output not the same len: got %d want %d, row %d", c.desc, got, len(c.wantTags), rx)
		} else {
			for i, row := range gotTags {
				if got := len(row); got != len(c.wantTags[i]) {
					t.Errorf("%s: tags output not same len for row(%d,%d): got %d want %d", c.desc, rx, i, got, len(c.wantTags[i]))
				} else {
					for j, tag := range row {
						want := c.wantTags[i][j]
						if got := tag; got != want {
							t.Errorf("%s: tag incorrect at row(%d,%d), %d: got %s want %s", c.desc, rx, i, j, got, want)
						}
					}
				}
			}
		}

		if got := len(gotData); got != len(c.wantData) {
			t.Errorf("%s: data ouput not the same len: got %d want %d, row %d", c.desc, got, len(c.wantData), rx)
		} else {
			for i, row := range gotData {
				if got := len(row); got != len(c.wantData[i]) {
					t.Errorf("%s: data output not same len for row(%d,%d): got %d want %d", c.desc, rx, i, got, len(c.wantData[i]))
				} else {
					for j, metric := range row {
						want := c.wantData[i][j]
						var got interface{}
						if j == 0 {
							got = metric.(time.Time).Format(time.RFC3339)
						} else if j == 3 {
							if !reflect.DeepEqual(metric, want) {
								t.Errorf("%s: incorrect additional tags: got %v want %v, row(%d,%d,%d)", c.desc, metric, want, rx, i, j)
							}
							continue
						} else {
							got = metric
						}
						if got != want {
							t.Errorf("%s: data incorrect at row(%d,%d,%d): got %v want %v", c.desc, rx, i, j, got, want)
						}
					}
				}
			}
		}
	}
}

func TestConvertValsToSQLBasedOnType(t *testing.T) {
	inVals := []string{"1", "2", "3", "4", "5", ""}
	inTypes := []string{"text", "int32", "int64", "float32", "float64", "int32"}
	converted := convertValsToSQLBasedOnType(inVals, inTypes)
	expected := []string{"'1'", "2", "3", "4", "5", "NULL"}
	if reflect.DeepEqual(expected, converted) {
		t.Errorf("error converting to sql values\nexpected: %v\ngot: %v", expected, converted)
	}
}
