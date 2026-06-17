package websocket

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"unicode"
)

type Envelope struct {
	URL     string                 `json:"url,omitempty"`
	Session map[string]interface{} `json:"session,omitempty"`
	Body    string                 `json:"body,omitempty"`
}

func NewOutboundEnvelope(url string, session map[string]interface{}, payload []byte) Envelope {
	return Envelope{
		URL:     url,
		Session: session,
		Body:    base64.StdEncoding.EncodeToString(payload),
	}
}

func (e *Envelope) Payload() ([]byte, error) {
	if e.Body == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(e.Body)
}

func DecodeEnvelope(data []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

func EncodeEnvelope(env Envelope) ([]byte, error) {
	return json.Marshal(env)
}

func SessionFromParams(params map[string]string, uuid string) map[string]interface{} {
	return sessionFromParams(params, uuid)
}

func sessionFromParams(params map[string]string, uuid string) map[string]interface{} {
	session := map[string]interface{}{
		"uuid": uuid,
	}
	for k, v := range params {
		session[toSessionKey(k)] = v
	}
	return session
}

func toSessionKey(param string) string {
	if param == "" {
		return param
	}
	runes := []rune(param)
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}

func matchSession(filter, target map[string]interface{}) bool {
	if len(filter) == 0 {
		return true
	}
	for k, fv := range filter {
		if strings.EqualFold(k, "uuid") {
			k = "uuid"
		}
		tv, ok := target[k]
		if !ok {
			return false
		}
		if toString(fv) != toString(tv) {
			return false
		}
	}
	return true
}

func toString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}
