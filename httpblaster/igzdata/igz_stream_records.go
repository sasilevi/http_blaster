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

type StreamRecord map[string]interface {
}

func (sr StreamRecord) GetData() string {
	data, err := base64.StdEncoding.DecodeString(sr[DATA].(string))
	if err != nil {
		panic(err)
	}
	return string(data)
}

func (sr StreamRecord) SetData(data string) {
	sr[DATA] = base64.StdEncoding.EncodeToString([]byte(data))
}

func (sr StreamRecord) SetClientInfo(clinetInfo string) {
	sr[ClientInfo] = clinetInfo
}

func (sr StreamRecord) SetPartitionKey(partitionKey string) {
	sr[PartitionKey] = partitionKey
}

func (sr StreamRecord) SetShardID(shardID int) {
	sr[ShardID] = shardID
}

type StreamRecords struct {
	//NextLocation int
	//LagInBytes   int
	//LagInRecord  int
	//RecordsNum   int
	Records []StreamRecord
}

func (sr *StreamRecords) ToJSONString() string {
	body, err := json.Marshal(sr)
	if err != nil {
		panic(err)
	}
	return string(body)
}

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

func NewStreamRecords(record StreamRecord) StreamRecords {
	r := StreamRecords{}
	r.Records = append(r.Records, record)
	return r
}
