package openapi

import (
	"encoding/json"
	"net/http"
	"testing"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// TestParamsStayRawJSON locks D1033: params are json.RawMessage, so a decimal
// string is not parsed as a float and written back.
func TestParamsStayRawJSON(t *testing.T) {
	var created apiv1.SessionCreate
	if err := json.Unmarshal([]byte(`{"params":{"px":"1.0"}}`), &created); err != nil {
		t.Fatal(err)
	}
	if created.Params == nil || string(*created.Params) != `{"px":"1.0"}` {
		t.Fatalf("create params = %s", created.Params)
	}

	view := apiv1.SessionSpecView{Params: json.RawMessage(`{"px":"1.0"}`)}
	raw, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var back apiv1.SessionSpecView
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if string(back.Params) != `{"px":"1.0"}` {
		t.Fatalf("view params = %s", back.Params)
	}
}

// TestServerIsStdHTTP locks D1044: the strict server is a net/http ServeMux,
// and the SSE event schemas survive generation.
func TestServerIsStdHTTP(t *testing.T) {
	var _ apiv1.SessionRemovedEvent
	var _ apiv1.ResetEvent

	handler := apiv1.Handler(apiv1.NewStrictHandler(nil, nil))
	if _, ok := handler.(*http.ServeMux); !ok {
		t.Fatalf("handler type %T, want *http.ServeMux", handler)
	}
	client, err := apiv1.NewClient("http://api.example")
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatal("client")
	}
}
