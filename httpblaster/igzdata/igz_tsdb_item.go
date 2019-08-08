package igzdata

import (
	"encoding/json"
	"fmt"
	"strconv"
)

//Sample Sample
type Sample struct {
	T string             `json:"t"`
	V map[string]float64 `json:"v"`
}

//IgzTSDBItem IgzTSDBItem
type IgzTSDBItem struct {
	Metric string `json:"metric"`

	Labels  map[string]string `json:"labels"`
	Samples []Sample          `json:"samples"`
}

func (ti *IgzTSDBItem) generateStruct(vals []string, parser *EmdSchemaParser) error {
	ti.insertParserMetric(vals, parser)
	ti.insertParserLables(vals, parser)
	ti.insertParserSample(vals, parser)
	return nil
}

//IgzTSDBItems2 IgzTSDBItems2
type IgzTSDBItems2 struct {
	Items []IgzTSDBItem
}

//InsertMetric InsertMetric
func (ti *IgzTSDBItem) InsertMetric(metric string) {
	ti.Metric = metric
}

//InsertLable InsertLable
func (ti *IgzTSDBItem) InsertLable(key string, value string) {
	if len(ti.Labels) == 0 {
		ti.Labels = make(map[string]string)
	}
	ti.Labels[key] = value
}

//InsertLables InsertLables
func (ti *IgzTSDBItem) InsertLables(lables map[string]string) {
	ti.Labels = lables
}

//InsertSample InsertSample
func (ti *IgzTSDBItem) InsertSample(ts string, value float64) {
	s := &Sample{}
	s.T = ts
	s.V = map[string]float64{"n": value}
	ti.Samples = append(ti.Samples, *s)
}

//ToJSONString ToJSONString
func (ti *IgzTSDBItem) ToJSONString() string {
	body, _ := json.Marshal(ti)
	return string(body)
}

//InsertParserMetric InsertParserMetric
func (ti *IgzTSDBItem) insertParserMetric(vals []string, parser *EmdSchemaParser) {
	parser.tsdbNameIndex = getIndexByValue(parser.valuesMap, parser.tsdbName)
	input := ""
	if parser.tsdbNameIndex > -1 {
		input = vals[parser.tsdbNameIndex]
	} else {
		input = parser.tsdbName
	}
	ti.InsertMetric(input)
}

func (ti *IgzTSDBItem) insertParserLables(vals []string, parser *EmdSchemaParser) {
	for key, val := range parser.tsdbAttributesMap {
		ti.InsertLable(key, vals[val])
	}
}

func (ti *IgzTSDBItem) insertParserSample(vals []string, parser *EmdSchemaParser) {
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

func getIndexByValue(vals map[int]schemaValue, val string) int {
	for _, v := range vals {
		if v.Name == val {
			return v.Index
		}
	}
	return -1
}
