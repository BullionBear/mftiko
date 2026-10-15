package exchangetest

import (
	"io"
	"net/http"

	"github.com/BullionBear/mftiko/exchange"
)

// HTTP serves the whole venue at one address, for a worker in another process: the private
// websocket at /private, as Handler does, and the REST Calls on every other path, answered as Do
// answers them. Run it with httptest.NewServer and put the server's URL in
// AccountConfig.RESTEndpoint and its ws:// URL in AccountConfig.StreamEndpoint; the worker sends the
// Calls with a real Doer.
//
// A request Do gives no answer to (Outcome.NoAnswer, Outcome.Late) has its connection dropped, so
// the Doer sees it as sent with no answer.
func (v *AccountVenue) HTTP() http.Handler {
	stream := v.Handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/private" {
			stream.ServeHTTP(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "body", http.StatusBadRequest)
			return
		}
		req := &exchange.HTTPRequest{Method: r.Method, Base: v.base, Path: r.URL.Path, Query: []byte(r.URL.RawQuery), Body: body}
		for name, vals := range r.Header {
			for _, val := range vals {
				req.Header = append(req.Header, exchange.Header{Name: name, Value: val})
			}
		}
		resp, err := v.Do(r.Context(), req)
		if err != nil {
			panic(http.ErrAbortHandler) // the connection drops with no response
		}
		for _, h := range resp.Header {
			w.Header().Add(h.Name, h.Value)
		}
		w.WriteHeader(resp.Status)
		w.Write(resp.Body) //nolint:errcheck // the client went away
	})
}
