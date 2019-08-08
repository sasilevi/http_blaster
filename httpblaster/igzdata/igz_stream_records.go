package igzdata

import (
	"encoding/base64"
	"encoding/json"
)

const (
	ClientInfo   = "ClientInfo"
	DATA         = "Data"
	PartitionKey = "PartitionKey"
	ShardId      = "ShardId"
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

func (sr StreamRecord) SetShardId(shardID int) {
	sr[ShardId] = shardID
}

type StreamRecords struct {
	//NextLocation int
	//LagInBytes   int
	//LagInRecord  int
	//RecordsNum   int
	Records []StreamRecord
}

func (sr *StreamRecords) ToJsonString() string {
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
		r.SetShardId(shardID)
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
