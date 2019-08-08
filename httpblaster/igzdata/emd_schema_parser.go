package igzdata

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/buger/jsonparser"
	uuid "github.com/nu7hatch/gouuid"
	"github.com/v3io/http_blaster/httpblaster/config"
)

type Schema struct {
	Settings SchemaSettings
	Columns  []SchemaValue
}

type SchemaSettings struct {
	Format       string
	Separator    config.Sep
	KeyFields    string
	KeyFormat    string
	UpdateFields string
	TSDBName     string
	TSDBTime     string
	TSDBValue    string
	TSDBLables   string
}

type SchemaValue struct {
	Name     string
	Type     IgzType
	Index    int
	Source   string
	Target   string
	Nullable bool
	Default  string
}

type EmdSchemaParser struct {
	SchemaFile         string
	valuesMap          map[int]SchemaValue
	schemaKeyIndexs    []int
	schemaKeyFormat    string
	schemaKeyFields    string
	JSONSchema         Schema
	updateFields       string
	updateFieldsIndexs []int
	updateMode         string
	updateExpression   string
	tsdbName           string
	tsdbNameIndex      int
	tsdbTime           string
	tsdbTimeIndex      int
	tsdbValue          string
	tsdbValueIndex     int
	tsdbAttributes     string
	tsdbAttributesMap  map[string]int
}

func (e *EmdSchemaParser) LoadSchema(filePath, updateMode, updateExpression string) error {

	e.valuesMap = make(map[int]SchemaValue)
	e.tsdbAttributesMap = make(map[string]int)
	plan, _ := ioutil.ReadFile(filePath)
	err := json.Unmarshal(plan, &e.JSONSchema)
	if err != nil {
		panic(err)
	}
	columns := e.JSONSchema.Columns
	settings := e.JSONSchema.Settings

	e.schemaKeyFormat = settings.KeyFormat
	e.schemaKeyFields = settings.KeyFields
	e.updateMode = updateMode
	e.updateExpression = updateExpression
	e.tsdbTime = settings.TSDBTime
	e.tsdbName = settings.TSDBName
	e.tsdbValue = settings.TSDBValue
	e.tsdbAttributes = settings.TSDBLables

	for _, v := range columns {
		e.valuesMap[v.Index] = v
	}
	e.GetKeyIndexes()
	e.MapTSDBLablesIndexes()
	e.GetTSDBNameIndex()
	e.GetTSDBValueIndex()
	if len(e.updateExpression) > 0 {
		e.GetUpdateExpressionIndexes()
	}
	return nil
}

func (e *EmdSchemaParser) GetUpdateExpressionIndexes() {
	r := regexp.MustCompile(`\$[a-zA-Z_]+`)
	matches := r.FindAllString(e.updateExpression, -1)

	for _, key := range matches {
		e.updateExpression = strings.Replace(e.updateExpression, key, "%v", 1)
		k := strings.Trim(key, "$")
		for i, v := range e.valuesMap {
			if v.Name == k {
				e.updateFieldsIndexs = append(e.updateFieldsIndexs, i)
			}
		}
	}
}

func (e *EmdSchemaParser) GetKeyIndexes() {
	keys := strings.Split(e.schemaKeyFields, ",")
	for _, key := range keys {
		for i, v := range e.valuesMap {
			if v.Name == key {
				e.schemaKeyIndexs = append(e.schemaKeyIndexs, i)
			}
		}
	}
}

func (e *EmdSchemaParser) GetTSDBNameIndex() {
	for _, v := range e.valuesMap {
		if v.Name == e.tsdbName {
			e.tsdbNameIndex = v.Index
		}
	}
}

func (e *EmdSchemaParser) GetTSDBValueIndex() {
	for _, v := range e.valuesMap {
		if v.Name == e.tsdbValue {
			e.tsdbValueIndex = v.Index
		}
	}
}

func (e *EmdSchemaParser) MapTSDBLablesIndexes() {
	attributes := strings.Split(e.tsdbAttributes, ",")
	for _, att := range attributes {
		for _, v := range e.valuesMap {
			if v.Name == att {
				e.tsdbAttributesMap[att] = v.Index
			}
		}
	}
}

func (e *EmdSchemaParser) GetFieldsIndexes(fields, delimiter string) []int {
	keys := strings.Split(fields, delimiter)
	indexArray := make([]int, 1)

	for _, key := range keys {
		for i, v := range e.valuesMap {
			if v.Name == key {
				indexArray = append(indexArray, i)
			}
		}
	}
	return indexArray
}

func (e *EmdSchemaParser) KeyFromCSVRecord(vals []string) string {
	//when no keys, generate random
	if len(e.schemaKeyIndexs) == 0 {
		u, _ := uuid.NewV4()
		return u.String()
	}
	//when 1 key, return the key
	if len(e.schemaKeyIndexs) == 1 {
		//fix bug of returning always key in position 0
		return vals[e.schemaKeyIndexs[0]]
	}
	//when more the one key, generate formatted key
	var keys []interface{}
	for _, i := range e.schemaKeyIndexs {
		keys = append(keys, vals[i])
	}
	key := fmt.Sprintf(e.schemaKeyFormat, keys...)
	return key
}

func (e *EmdSchemaParser) nameIndexFromCSVRecord(vals []string) string {
	//when no keys, generate random
	if len(e.schemaKeyIndexs) == 0 {
		u, _ := uuid.NewV4()
		return u.String()
	}
	//when 1 key, return the key
	if len(e.schemaKeyIndexs) == 1 {
		//fix bug of returning always key in position 0
		return vals[e.schemaKeyIndexs[0]]
	}
	//when more the one key, generate formatted key
	var keys []interface{}
	for _, i := range e.schemaKeyIndexs {
		keys = append(keys, vals[i])
	}
	key := fmt.Sprintf(e.schemaKeyFormat, keys...)
	return key
}

func (e *EmdSchemaParser) EmdFromCSVRecord(vals []string) string {
	emdItem := NewEmdItem()
	emdItem.InsertKey("key", TSTRING, e.KeyFromCSVRecord(vals))
	for i, v := range vals {
		if val, ok := e.valuesMap[i]; ok {
			err, igzType, value := ConvertValue(val.Type, v)
			if err != nil {
				panic(fmt.Sprintf("conversion error %d %v %v", i, v, e.valuesMap[i]))
			}
			emdItem.InsertItemAttr(e.valuesMap[i].Name, igzType, value)
		}
	}
	return string(emdItem.ToJsonString())
}

func (e *EmdSchemaParser) TSDBFromCSVRecord(vals []string) string {
	tsdbItem := IgzTSDBItem{}
	tsdbItem.GenerateStruct(vals, e)
	return string(tsdbItem.ToJsonString())
}

func (e *EmdSchemaParser) TSDBItemsFromCSVRecord(vals []string) []string {
	tsdbItem := IgzTSDBItem{}
	tsdbItem.GenerateStruct(vals, e)
	//return string(tsdbItem.ToJsonString())
	return nil
}

func (e *EmdSchemaParser) EmdUpdateFromCSVRecord(vals []string) string {
	emdUpdate := NewEmdItemUpdate()
	//emdUpdate.InsertKey("key", TSTRING, e.KeyFromCSVRecord(vals))
	emdUpdate.UpdateMode = e.updateMode
	var fields []interface{}
	for _, i := range e.updateFieldsIndexs {
		fields = append(fields, vals[i])
	}
	if len(fields) > 0 {
		emdUpdate.UpdateExpression = fmt.Sprintf(e.updateExpression, fields...)
	} else {
		emdUpdate.UpdateExpression = e.updateExpression
	}
	return string(emdUpdate.ToJsonString())
}

func (e *EmdSchemaParser) HandleJsonSource(source string) []string {
	var out []string
	arr := strings.Split(source, ".")
	for _, a := range arr {
		out = append(out, handleOffset(a)...)
	}
	return out
}

func handleOffset(str string) []string {
	var res []string
	vls := strings.Split(str, "]")
	if len(vls) == 1 && !strings.HasSuffix(str, "]") {
		res = append(res, vls...)
	}
	for _, k := range vls {
		if strings.HasPrefix(k, "[") {
			res = append(res, k+"]")
		} else {
			vl := strings.Split(k, "[")
			if len(vl) == 2 {
				res = append(res, vl[0])
				res = append(res, "["+vl[1]+"]")
			}
		}
	}
	return res
}

func (e *EmdSchemaParser) KeyFromJsonRecord(jsonObj []byte) string {
	//when no keys, generate random
	if len(e.schemaKeyIndexs) == 0 {
		u, _ := uuid.NewV4()
		return u.String()
	}
	//when 1 key, return the key
	if len(e.schemaKeyIndexs) == 1 {
		sourceArr := e.HandleJsonSource(e.valuesMap[e.schemaKeyIndexs[0]].Source)
		s, _, _, e := jsonparser.Get(jsonObj, sourceArr...)
		if e != nil {
			panic(fmt.Sprintf("%v, %+v", e, sourceArr))
		}
		return string(s)
	}
	//when more the one key, generate formatted key
	var keys []interface{}
	for _, i := range e.schemaKeyIndexs {
		//fmt.Println("indexes ",i, len(e.valuesMap))
		sourceArr := e.HandleJsonSource(e.valuesMap[i].Source)
		s, _, _, e := jsonparser.Get(jsonObj, sourceArr...)
		if e != nil {
			panic(e)
		} else {
			keys = append(keys, string(s))
		}
	}
	key := fmt.Sprintf(e.schemaKeyFormat, keys...)
	return key
}

func (e *EmdSchemaParser) EmdFromJsonRecord(jsonObj []byte) (string, error) {
	emdItem := NewEmdItem()
	emdItem.InsertKey("key", TSTRING, e.KeyFromJsonRecord(jsonObj))
	for _, v := range e.valuesMap {
		sourceArr := e.HandleJsonSource(v.Source)
		var str []byte
		var e error
		str, _, _, e = jsonparser.Get(jsonObj, sourceArr...)
		if e != nil {
			if e == jsonparser.KeyPathNotFoundError {
				if v.Nullable {
					continue
				} else if v.Default != "" {
					str = []byte(v.Default)
				} else {
					return "", errors.New(fmt.Sprintf("%v, %+v", e, v.Source))
				}
			} else {
				return "", errors.New(fmt.Sprintf("%v, %+v", e, v.Source))
			}
		}
		err, igzType, value := ConvertValue(v.Type, string(str))
		if err != nil {
			return "", errors.New(fmt.Sprintf("%v, %+v", err, v.Source))
		}
		emdItem.InsertItemAttr(v.Name, igzType, value)
	}
	return string(emdItem.ToJsonString()), nil
}

func ConvertValue(t IgzType, v string) (error, IgzType, interface{}) {
	switch t {
	case TSTRING:
		return nil, TSTRING, v
	case TNUMBER:
		return nil, TNUMBER, v
	case TDOUBLE:
		//r, e := strconv.ParseFloat(v, 64)
		//if e != nil {
		//	panic(e)
		//}
		//val := fmt.Sprintf("%.1f", r)
		//return e, TNUMBER, val
		return nil, TNUMBER, v
	default:
		return errors.New(fmt.Sprintf("missing type conversion %v", t)), TSTRING, ""
	}
}
