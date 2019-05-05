package dto

import (
	"encoding/gob"
)

//UserAgentMessage : User agent message from bquery reading with  user agent string and platform
type UserAgentMessage struct {
	UserAgent string
	Platform  string
}

func init() {
	gob.Register(UserAgentMessage{})
}
