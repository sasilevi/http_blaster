package dto

import (
	"encoding/gob"
	"sync"
)

//UserAgentMessage : User agent message from bquery reading with  user agent string and platform
type UserAgentMessage struct {
	UserAgent         string
	Target            string
	BodyHash          uint32
	ExpectedStoreLink string
	WrongLink         bool
	NotFound          bool
}

func init() {
	gob.Register(UserAgentMessage{})
}

var (
	uaPool sync.Pool
)

// AcquireUaObj : Retrive ua object to use from pool
func AcquireUaObj(ua, target string) *UserAgentMessage {
	v := uaPool.Get()

	if v == nil {
		return &UserAgentMessage{UserAgent: ua, Target: target, WrongLink: false, NotFound: false, BodyHash: 0}
	}

	v.(*UserAgentMessage).Target = target
	v.(*UserAgentMessage).UserAgent = ua
	v.(*UserAgentMessage).WrongLink = false
	v.(*UserAgentMessage).NotFound = false
	v.(*UserAgentMessage).BodyHash = 0

	return v.(*UserAgentMessage)
}

//ReleaseUaObj : Release ua object back to pool
func ReleaseUaObj(ua *UserAgentMessage) {
	uaPool.Put(ua)
}
