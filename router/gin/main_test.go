package gin

import (
	"os"
	"testing"

	ws "github.com/pucora/velonetics-websocket/v2"
)

func TestMain(m *testing.M) {
	code := m.Run()
	ws.ResetHubRegistry()
	os.Exit(code)
}
