package dto

import (
	"encoding/gob"
	"sync"
)

//UserAgentMessage : User agent message from bquery reading with  user agent string and platform
type UserAgentMessage struct {
	UserAgent string
	Target    string
}

func init() {
	gob.Register(UserAgentMessage{})
}

var (
	uaPool sync.Pool
)

func AcquireUaObj(ua, target string) *UserAgentMessage {
	v := uaPool.Get()
	if v == nil {
		return &UserAgentMessage{UserAgent: ua, Target: target}
	}
	v.(*UserAgentMessage).Target = target
	v.(*UserAgentMessage).UserAgent = ua
	return v.(*UserAgentMessage)
}

func ReleaseUaObj(ua *UserAgentMessage) {
	uaPool.Put(ua)
}
