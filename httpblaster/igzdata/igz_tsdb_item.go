package igzdata

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type Sample struct {
	T string             `json:"t"`
	V map[string]float64 `json:"v"`
}

type IgzTSDBItem struct {
	Metric string `json:"metric"`

	Labels  map[string]string `json:"labels"`
	Samples []Sample          `json:"samples"`
}

func (ti *IgzTSDBItem) GenerateStruct(vals []string, parser *EmdSchemaParser) error {
	ti.InsertParserMetric(vals, parser)
	ti.InsertParserLables(vals, parser)
	ti.InsertParserSample(vals, parser)
	return nil
}

type IgzTSDBItems2 struct {
	Items []IgzTSDBItem
}

func (ti *IgzTSDBItem) InsertMetric(metric string) {
	ti.Metric = metric
}

func (ti *IgzTSDBItem) InsertLable(key string, value string) {
	if len(ti.Labels) == 0 {
		ti.Labels = make(map[string]string)
	}
	ti.Labels[key] = value
}

func (ti *IgzTSDBItem) InsertLables(lables map[string]string) {
	ti.Labels = lables
}

func (ti *IgzTSDBItem) InsertSample(ts string, value float64) {
	s := &Sample{}
	s.T = ts
	s.V = map[string]float64{"n": value}
	ti.Samples = append(ti.Samples, *s)
}

func (ti *IgzTSDBItem) ToJsonString() string {
	body, _ := json.Marshal(ti)
	return string(body)
}

func (ti *IgzTSDBItem) InsertParserMetric(vals []string, parser *EmdSchemaParser) {
	parser.tsdbNameIndex = GetIndexByValue(parser.valuesMap, parser.tsdbName)
	input := ""
	if parser.tsdbNameIndex > -1 {
		input = vals[parser.tsdbNameIndex]
	} else {
		input = parser.tsdbName
	}
	ti.InsertMetric(input)
}

func (ti *IgzTSDBItem) InsertParserLables(vals []string, parser *EmdSchemaParser) {
	for key, val := range parser.tsdbAttributesMap {
		ti.InsertLable(key, vals[val])
	}
}

func (ti *IgzTSDBItem) InsertParserSample(vals []string, parser *EmdSchemaParser) {
	for _, v := range parser.valuesMap {
		if v.Name == parser.tsdbTime {
			parser.tsdbTimeIndex = v.Index
		}
	}
	ts := vals[parser.tsdbTimeIndex]
	val := vals[parser.tsdbValueIndex]
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		panic(fmt.Sprintf("conversion error to float %v ", val))
	}
	ti.InsertSample(ts, f)
}

func GetIndexByValue(vals map[int]SchemaValue, val string) int {
	for _, v := range vals {
		if v.Name == val {
			return v.Index
		}
	}
	return -1
}
