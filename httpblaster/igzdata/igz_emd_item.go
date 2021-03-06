package igzdata

import (
	"encoding/json"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"
)

//IgzType : iguazio data type
type IgzType string

// data types
const (
	TBLOB      IgzType = "B"
	TBOOL              = "BOOL"
	TATTRLIST          = "L"
	TATTRMAP           = "M"
	TNUMBER            = "N"
	TNUMBERSET         = "NS"
	TNULL              = "NULL"
	TUNIXTIME          = "UT"
	TTIMESTAMP         = "TS"
	TSTRING            = "S"
	TSTRINGSET         = "SS"
	TDOUBLE            = "D"
)

//IgzEmdItem : igzEmdItem
type IgzEmdItem struct {
	//TableName           string
	//ConditionExpression string
	Key  map[string]map[string]interface{}
	Item map[string]map[string]interface{}
}

//ToJSONString : ToJSONString
func (i *IgzEmdItem) ToJSONString() string {
	body, _ := json.Marshal(i)
	return string(body)
}

//InsertKey : InsertKey
func (i *IgzEmdItem) InsertKey(key string, valueType IgzType, value interface{}) error {
	if _, ok := i.Key[key]; ok {
		err := fmt.Sprintf("Key %s Override existing key %v", key, i.Key)
		log.Error(err)
		return errors.New(err)
	}
	i.Key[key] = make(map[string]interface{})
	i.Key[key][string(valueType)] = value
	return nil
}

//InsertItemAttr :InsertItemAttr
func (i *IgzEmdItem) InsertItemAttr(attrName string, valueType IgzType, value interface{}) error {
	if _, ok := i.Item[attrName]; ok {
		err := fmt.Sprintf("Key %s Override existing item %v", attrName, i.Item)
		log.Error(err)
		return errors.New(err)
	}
	i.Item[attrName] = make(map[string]interface{})
	i.Item[attrName][string(valueType)] = value
	return nil
}

//NewEmdItem :New Emd Item
func NewEmdItem() *IgzEmdItem {
	i := &IgzEmdItem{}
	i.Key = make(map[string]map[string]interface{})
	i.Item = make(map[string]map[string]interface{})
	return i
}

//IgzEmdItemUpdate :IgzEmdItemUpdate
type IgzEmdItemUpdate struct {
	//TableName           string
	UpdateMode       string
	UpdateExpression string
	//Key  map[string]map[string]interface{}
}

//
//func (i *IgzEmdItemUpdate) InsertKey(key string, valueType IgzType, value interface{}) error {
//	if _, ok := i.Key[key]; ok {
//		err := fmt.Sprintf("Key %s Override existing key %v", key, i.Key)
//		log.Error(err)
//		return errors.New(err)
//	}
//	i.Key[key] = make(map[string]interface{})
//	i.Key[key][string(valueType)] = value
//	return nil
//}

//ToJSONString :ToJSONString
func (i *IgzEmdItemUpdate) ToJSONString() string {
	body, _ := json.Marshal(i)
	return string(body)
}

//NewEmdItemUpdate :New Emd Item Update
func NewEmdItemUpdate() *IgzEmdItemUpdate {
	i := &IgzEmdItemUpdate{}
	//i.Key = make(map[string]map[string]interface{})
	return i
}

//IgzEmdItemQuery :IgzEmdItemQuery
type IgzEmdItemQuery struct {
	TableName       string
	AttributesToGet string
	Key             map[string]map[string]interface{}
}

//ToJSONString : ToJSONString
func (i *IgzEmdItemQuery) ToJSONString() string {
	body, _ := json.Marshal(i)
	return string(body)
}

//InsertKey : InsertKey
func (i *IgzEmdItemQuery) InsertKey(key string, valueType IgzType, value interface{}) error {
	if _, ok := i.Key[key]; ok {
		err := fmt.Sprintf("Key %s Override existing key %v", key, i.Key)
		log.Error(err)
		return errors.New(err)
	}
	i.Key[key] = make(map[string]interface{})
	i.Key[key][string(valueType)] = value
	return nil
}

//NewEmdItemQuery : New Emd Item Query
func NewEmdItemQuery() *IgzEmdItemQuery {
	q := &IgzEmdItemQuery{}
	q.Key = make(map[string]map[string]interface{})
	return q
}

//IgzEmdItemsQuery IgzEmdItemsQuery
type IgzEmdItemsQuery struct {
	TableName        string
	AttributesToGet  string
	Limit            int
	FilterExpression string
	Segment          int
	TotalSegment     int
	Marker           string
	StartingKey      map[string]map[string]interface{}
	EndingKey        map[string]map[string]interface{}
}

//ToJSONString : ToJSONString
func (i *IgzEmdItemsQuery) ToJSONString() string {
	body, _ := json.Marshal(i)
	return string(body)
}

//InsertStartingKey : InsertStartingKey
func (i *IgzEmdItemsQuery) InsertStartingKey(key string, valueType IgzType, value interface{}) error {
	if _, ok := i.StartingKey[key]; ok {
		err := fmt.Sprintf("Key %s Override existing key %v", key, i.StartingKey)
		log.Error(err)
		return errors.New(err)
	}
	i.StartingKey[key] = make(map[string]interface{})
	i.StartingKey[key][string(valueType)] = value
	return nil
}

//InsertEndingKey : InsertEndingKey
func (i *IgzEmdItemsQuery) InsertEndingKey(key string, valueType IgzType, value interface{}) error {
	if _, ok := i.EndingKey[key]; ok {
		err := fmt.Sprintf("Key %s Override existing key %v", key, i.EndingKey)
		log.Error(err)
		return errors.New(err)
	}
	i.EndingKey[key] = make(map[string]interface{})
	i.EndingKey[key][string(valueType)] = value
	return nil
}

//NewEmdItemsQuery : NewEmdItemsQuery
func NewEmdItemsQuery() *IgzEmdItemQuery {
	q := &IgzEmdItemQuery{}
	q.Key = make(map[string]map[string]interface{})
	return q
}
