package igzdata

import (
	"encoding/base64"
	"encoding/json"
)

//igz data consts
const (
	ClientInfo   = "ClientInfo"
	DATA         = "Data"
	PartitionKey = "PartitionKey"
	ShardID      = "ShardId"
)

//StreamRecord StreamRecord
type StreamRecord map[string]interface {
}

//GetData get record data as string
func (sr StreamRecord) GetData() string {
	data, err := base64.StdEncoding.DecodeString(sr[DATA].(string))
	if err != nil {
		panic(err)
	}
	return string(data)
}

//SetData : set record data
func (sr StreamRecord) SetData(data string) {
	sr[DATA] = base64.StdEncoding.EncodeToString([]byte(data))
}

//SetClientInfo : SetClientInfo
func (sr StreamRecord) SetClientInfo(clinetInfo string) {
	sr[ClientInfo] = clinetInfo
}

//SetPartitionKey :SetPartitionKey
func (sr StreamRecord) SetPartitionKey(partitionKey string) {
	sr[PartitionKey] = partitionKey
}

//SetShardID :SetShardID
func (sr StreamRecord) SetShardID(shardID int) {
	sr[ShardID] = shardID
}

//StreamRecords :StreamRecords
type StreamRecords struct {
	//NextLocation int
	//LagInBytes   int
	//LagInRecord  int
	//RecordsNum   int
	Records []StreamRecord
}

//ToJSONString : ToJSONString
func (sr *StreamRecords) ToJSONString() string {
	body, err := json.Marshal(sr)
	if err != nil {
		panic(err)
	}
	return string(body)
}

//NewStreamRecord : New Stream Record
func NewStreamRecord(clientInfo string, data string, partitionKey string,
	shardID int, shardRoundRobin bool) StreamRecord {

	r := StreamRecord{}
	r = make(map[string]interface{})
	r.SetClientInfo(clientInfo)
	if shardRoundRobin == false {
		r.SetShardID(shardID)
		r.SetPartitionKey(partitionKey)
	}
	r.SetData(data)
	return r
}

//NewStreamRecords :New Stream Records
func NewStreamRecords(record StreamRecord) StreamRecords {
	r := StreamRecords{}
	r.Records = append(r.Records, record)
	return r
}
