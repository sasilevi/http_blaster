package igzdata

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"time"

	"github.com/stretchr/testify/assert"
)

var item2 = IgzTSDBItem{}
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var metric = randSeq(10)
var lables = map[string]string{"dc": "7", "hostname": "myhosts"}
var timestamp = NowAsUnixMilli()
var floatVal = randFloat(0, rand.Float64())
var lableKey = "lable"
var lableVal = "lableValue"

func init() {
	item2.InsertMetric(metric)
	item2.InsertLable(lableKey, lableVal)
	item2.InsertSample(timestamp, floatVal)

	fmt.Println(item2.Samples)
	/* load test data */
}

func Test_igz_tsdb_item_v2_init(t *testing.T) {
	assert.Equal(t, metric, item2.Metric, "they should be equal")
}

func Test_igz_tsdb_item_v2_lables(t *testing.T) {
	assert.Equal(t, lableVal, item2.Labels[lableKey], "they should be equal")
}

func Test_igz_tsdb_item_v2_sample(t *testing.T) {
	assert.Equal(t, timestamp, item2.Samples[0].T, "they should be equal")
	assert.Equal(t, floatVal, item2.Samples[0].V["n"], "they should be equal")
}

func Test__igz_tsdb_item_v2_convert(t *testing.T) {
	print(item2.ToJSONString())
}

func NowAsUnixMilli() string {
	ts := time.Now().UnixNano() / 1e6
	tsStr := strconv.FormatInt(ts, 10)
	return tsStr
}

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func randFloat(min, max float64) float64 {
	res := min + rand.Float64()*(max-min)
	return res
}
