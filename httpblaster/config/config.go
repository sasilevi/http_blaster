/*
Copyright 2016 Iguazio.io Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/

package config

import (
	"time"

	"github.com/BurntSushi/toml"
)

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// Sep : seperator
type Sep struct {
	Rune rune
}

// UnmarshalText : seperator unmarshal text
func (r *Sep) UnmarshalText(text []byte) error {
	if len(text) > 0 {
		data := int32(text[0])
		r.Rune = data
	}
	return nil
}

// TomlConfig : configuration structure
type TomlConfig struct {
	Title     string
	Global    Global
	Workloads map[string]Workload
}

// Global : configuration global section
type Global struct {
	Duration              duration
	BlockSize             int32
	Servers               []string
	Server                string
	Port                  string
	PemFile               string
	TLSMode               bool
	StatusCodesAcceptance map[string]float64
	RetryOnStatusCodes    []int
	RetryCount            int
	IgnoreAttrs           []string
	RbmqAddr              string
	RbmqPort              string
	RbmqUser              string
	BqueryCert            string
	DbHost                string
	DbPort                int32
	DbName                string
	DbUser                string
	DbPassword            string
	DbRecordAll           bool
}

//Workload : Workload configuration
//			Args:
//				//Targets : if an map between target and expected response
type Workload struct {
	Name                      string
	Container                 string
	Target                    string
	Targets                   map[string]string
	Type                      string
	Duration                  duration
	Count                     int
	Workers                   int
	ID                        int
	Header                    map[string]string
	Payload                   string
	FileIndex                 int
	FilesCount                int
	Random                    bool
	Generator                 string
	ResponseHandler           string
	Schema                    string
	Lazy                      int
	ShardCount                uint32
	ShardColumn               uint32
	Separator                 string
	UpdateMode                string
	UpdateExpression          string
	Topic                     string
	MatchExpression           string
	Query                     string
	BQProjectID               string
	ResponseHandlerDumpToFile bool
	ImpersonateHosts          map[string]bool
	Args                      string
	ResetConnectionOnSend     bool
}

//LoadConfig : Load configuration file
//args: filePath : path for the config file
func LoadConfig(filePath string) (TomlConfig, error) {
	var config TomlConfig
	if _, err := toml.DecodeFile(filePath, &config); err != nil {
		return config, err
	}
	return config, nil
}
