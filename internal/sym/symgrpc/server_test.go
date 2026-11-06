package symgrpc

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	reflectpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	symv1 "github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1"
	"github.com/BullionBear/mftiko/contract/gen/mftiko/sym/v1/symv1connect"
	"github.com/BullionBear/mftiko/internal/sym"
)

const testToken = "s3cret-token"

// fakeAPI answers every read with a fixed message, or err when set. block, when set, holds a
// call until it is closed.
type fakeAPI struct {
	err   error
	block chan struct{}
	calls chan struct{}
}

func (f *fakeAPI) wait(ctx context.Context) error {
	if f.calls != nil {
		f.calls <- struct{}{}
	}
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return f.err
}

func info(uid uint32) *symv1.SymbolInfo {
	return &symv1.SymbolInfo{UniversalTicker: "Bybit_Spot_BTCUSDT", ExchTicker: "BTCUSDT", Uid: uid,
		Status: symv1.SymbolStatus_SYMBOL_STATUS_TRADABLE}
}

func (f *fakeAPI) Get(ctx context.Context, req *symv1.GetRequest) (*symv1.SymbolInfo, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	return info(1), nil
}

func (f *fakeAPI) GetByUID(ctx context.Context, req *symv1.GetByUidRequest) (*symv1.SymbolInfo, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	return info(req.GetUid()), nil
}

func (f *fakeAPI) List(ctx context.Context, req *symv1.ListRequest) (*symv1.SymbolList, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	return &symv1.SymbolList{Symbols: []*symv1.SymbolInfo{info(1), info(2)}, Total: 7}, nil
}

func (f *fakeAPI) Reverse(ctx context.Context, req *symv1.ReverseRequest) (*symv1.SymbolInfo, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	return info(3), nil
}

func (f *fakeAPI) Revisions(ctx context.Context, req *symv1.RevisionsRequest) (*symv1.RevisionList, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	return &symv1.RevisionList{Tables: []*symv1.TableRevision{{Venue: "Bybit", Category: "Spot", Revision: 4}}}, nil
}

func (f *fakeAPI) Refresh(context.Context, *symv1.RefreshRequest) (*symv1.RefreshResult, error) {
	return nil, errors.New("refresh is not on contract S")
}

func (f *fakeAPI) Watch(context.Context, sym.WatchFilter) (*sym.Watch, error) {
	return nil, &sym.Error{Code: sym.CodeUnavailable, Reason: "watch is not served by this process"}
}

// syncBuffer is a log sink the server writes from its own goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type served struct {
	addr string
	log  *syncBuffer
	stop context.CancelFunc
	done chan error
}

func serve(t *testing.T, api sym.API, tlsCfg *tls.Config) *served {
	t.Helper()
	tokens, err := ParseTokens(strings.NewReader("# who calls\npms " + testToken + "\n\nops other-token\n"))
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	srv, err := New(api, Config{Tokens: tokens, TLS: tlsCfg, Log: zerolog.New(logs), ShutdownTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	s := &served{addr: ln.Addr().String(), log: logs, stop: cancel, done: done}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("server did not stop")
		}
	})
	return s
}

// dial is a stock grpc-go client over h2c: the compatibility contract S promises (E22-D005).
func dial(t *testing.T, addr string, creds credentials.TransportCredentials) *grpc.ClientConn {
	t.Helper()
	if creds == nil {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func withToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func TestGRPCClientCallsEveryRPC(t *testing.T) {
	s := serve(t, &fakeAPI{}, nil)
	conn := dial(t, s.addr, nil)
	ctx := withToken(t.Context(), testToken)

	var get symv1.GetResponse
	if err := conn.Invoke(ctx, symv1connect.SymServiceGetProcedure, &symv1.GetRequest{UniversalTicker: "Bybit_Spot_BTCUSDT"}, &get); err != nil {
		t.Fatal(err)
	}
	if get.GetInfo().GetUid() != 1 || get.GetInfo().GetExchTicker() != "BTCUSDT" {
		t.Fatalf("get %v", &get)
	}
	var byUID symv1.GetByUidResponse
	if err := conn.Invoke(ctx, symv1connect.SymServiceGetByUidProcedure, &symv1.GetByUidRequest{Uid: 9}, &byUID); err != nil {
		t.Fatal(err)
	}
	if byUID.GetInfo().GetUid() != 9 {
		t.Fatalf("get by uid %v", &byUID)
	}
	var list symv1.ListResponse
	if err := conn.Invoke(ctx, symv1connect.SymServiceListProcedure, &symv1.ListRequest{Venue: "Bybit", Category: "Spot"}, &list); err != nil {
		t.Fatal(err)
	}
	if list.GetList().GetTotal() != 7 || len(list.GetList().GetSymbols()) != 2 {
		t.Fatalf("list %v", &list)
	}
	var rev symv1.ReverseResponse
	if err := conn.Invoke(ctx, symv1connect.SymServiceReverseProcedure, &symv1.ReverseRequest{Venue: "Bybit", Category: "Spot", ExchTicker: "BTCUSDT"}, &rev); err != nil {
		t.Fatal(err)
	}
	if rev.GetInfo().GetUid() != 3 {
		t.Fatalf("reverse %v", &rev)
	}
	var revs symv1.RevisionsResponse
	if err := conn.Invoke(ctx, symv1connect.SymServiceRevisionsProcedure, &symv1.RevisionsRequest{}, &revs); err != nil {
		t.Fatal(err)
	}
	if got := revs.GetRevisions().GetTables(); len(got) != 1 || got[0].GetRevision() != 4 {
		t.Fatalf("revisions %v", &revs)
	}

	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, symv1connect.SymServiceWatchChangesProcedure)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(&symv1.WatchChangesRequest{}); err != nil {
		t.Fatal(err)
	}
	_ = stream.CloseSend()
	err = stream.RecvMsg(&symv1.WatchChangesResponse{})
	if st := grpcstatus.Convert(err); st.Code() != codes.Unavailable || st.Message() != "unavailable: watch is not served by this process" {
		t.Fatalf("watch changes: %v", err)
	}
}

func TestTokens(t *testing.T) {
	s := serve(t, &fakeAPI{}, nil)
	conn := dial(t, s.addr, nil)
	req := &symv1.GetRequest{UniversalTicker: "Bybit_Spot_BTCUSDT"}
	cases := []struct {
		name string
		ctx  context.Context
		code codes.Code
		msg  string
	}{
		{"none", t.Context(), codes.Unauthenticated, "bearer token required"},
		{"wrong", withToken(t.Context(), "nope"), codes.Unauthenticated, "invalid bearer token"},
		{"not bearer", metadata.AppendToOutgoingContext(t.Context(), "authorization", "Basic "+testToken), codes.Unauthenticated, "bearer token required"},
		{"second name", withToken(t.Context(), "other-token"), codes.OK, ""},
	}
	for _, tc := range cases {
		err := conn.Invoke(tc.ctx, symv1connect.SymServiceGetProcedure, req, &symv1.GetResponse{})
		st := grpcstatus.Convert(err)
		if st.Code() != tc.code || st.Message() != tc.msg {
			t.Fatalf("%s: %v %q, want %v %q", tc.name, st.Code(), st.Message(), tc.code, tc.msg)
		}
	}
	waitLog(t, s.log, `"name":"ops"`)
	if !strings.Contains(s.log.String(), `"rpc":"/mftiko.sym.v1.SymService/Get","name":"","peer":`) ||
		!strings.Contains(s.log.String(), `"code":"unauthenticated"`) {
		t.Fatalf("access log %s", s.log.String())
	}
	if strings.Contains(s.log.String(), testToken) || strings.Contains(s.log.String(), "other-token") {
		t.Fatal("a token reached the log")
	}
}

func waitLog(t *testing.T, b *syncBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(b.String(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("log has no %s: %s", want, b.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestErrorStatus pins E22-D004: the code per business error, and the same text the sym subject sends.
func TestErrorStatus(t *testing.T) {
	cases := []struct {
		err  error
		code codes.Code
	}{
		{&sym.Error{Code: sym.CodeInvalid, Reason: "limit 600 exceeds max 500"}, codes.InvalidArgument},
		{&sym.Error{Code: sym.CodeNotFound, Reason: "uid 7 not found in active table (include_inactive=false)"}, codes.NotFound},
		{&sym.Error{Code: sym.CodeUnavailable, Reason: "list Bybit/Spot: database read failed", Cause: errors.New("SELECT secret")}, codes.Unavailable},
		{&sym.Error{Code: sym.CodeUnavailable, Reason: "get uid 7: read: context canceled", Cause: fmt.Errorf("read: %w", context.Canceled)}, codes.Canceled},
		{&sym.Error{Code: sym.CodeUnavailable, Reason: "get uid 7: context deadline exceeded", Cause: context.DeadlineExceeded}, codes.DeadlineExceeded},
	}
	for _, tc := range cases {
		s := serve(t, &fakeAPI{err: tc.err}, nil)
		conn := dial(t, s.addr, nil)
		err := conn.Invoke(withToken(t.Context(), testToken), symv1connect.SymServiceGetByUidProcedure, &symv1.GetByUidRequest{Uid: 7}, &symv1.GetByUidResponse{})
		st := grpcstatus.Convert(err)
		if st.Code() != tc.code || st.Message() != tc.err.Error() {
			t.Fatalf("got %v %q, want %v %q", st.Code(), st.Message(), tc.code, tc.err.Error())
		}
		if strings.Contains(st.Message(), "SELECT") {
			t.Fatal("the cause reached the client")
		}
	}
	if err := status(errors.New("not a business error")); connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("plain error: %v", err)
	}
}

func TestReflectionListsSymService(t *testing.T) {
	s := serve(t, &fakeAPI{}, nil)
	conn := dial(t, s.addr, nil)
	client := reflectpb.NewServerReflectionClient(conn)

	denied, err := client.ServerReflectionInfo(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_ = denied.Send(&reflectpb.ServerReflectionRequest{MessageRequest: &reflectpb.ServerReflectionRequest_ListServices{}})
	if _, err := denied.Recv(); grpcstatus.Code(err) != codes.Unauthenticated {
		t.Fatalf("reflection without a token: %v", err)
	}

	stream, err := client.ServerReflectionInfo(withToken(t.Context(), testToken))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&reflectpb.ServerReflectionRequest{MessageRequest: &reflectpb.ServerReflectionRequest_ListServices{}}); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, svc := range resp.GetListServicesResponse().GetService() {
		names = append(names, svc.GetName())
	}
	if !slices.Contains(names, symv1connect.SymServiceName) || !slices.Contains(names, "grpc.health.v1.Health") {
		t.Fatalf("services %v", names)
	}
	if err := stream.Send(&reflectpb.ServerReflectionRequest{MessageRequest: &reflectpb.ServerReflectionRequest_FileContainingSymbol{
		FileContainingSymbol: symv1connect.SymServiceName,
	}}); err != nil {
		t.Fatal(err)
	}
	resp, err = stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	var methods []string
	for _, raw := range resp.GetFileDescriptorResponse().GetFileDescriptorProto() {
		var fd descriptorpb.FileDescriptorProto
		if err := proto.Unmarshal(raw, &fd); err != nil {
			t.Fatal(err)
		}
		for _, svc := range fd.GetService() {
			if svc.GetName() != "SymService" {
				continue
			}
			for _, m := range svc.GetMethod() {
				methods = append(methods, m.GetName())
			}
		}
	}
	want := []string{"Get", "GetByUid", "List", "Reverse", "Revisions", "WatchChanges"}
	if !slices.Equal(methods, want) {
		t.Fatalf("methods %v, want %v", methods, want)
	}
}

// Health answers without a token, so a load balancer or a probe can ask (E22-D014).
func TestHealthNeedsNoToken(t *testing.T) {
	s := serve(t, &fakeAPI{}, nil)
	conn := dial(t, s.addr, nil)
	client := healthpb.NewHealthClient(conn)
	for _, service := range []string{"", symv1connect.SymServiceName} {
		resp, err := client.Check(t.Context(), &healthpb.HealthCheckRequest{Service: service})
		if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			t.Fatalf("health %q: %v %v", service, resp, err)
		}
	}
}

// A call in flight when the context ends still gets its answer; Serve returns after it (E22-D012).
func TestShutdownWaitsForCallsInFlight(t *testing.T) {
	api := &fakeAPI{block: make(chan struct{}), calls: make(chan struct{}, 1)}
	s := serve(t, api, nil)
	conn := dial(t, s.addr, nil)
	got := make(chan error, 1)
	go func() {
		got <- conn.Invoke(withToken(context.Background(), testToken), symv1connect.SymServiceRevisionsProcedure, &symv1.RevisionsRequest{}, &symv1.RevisionsResponse{})
	}()
	<-api.calls
	s.stop()
	select {
	case err := <-s.done:
		t.Fatalf("Serve returned with a call in flight: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(api.block)
	if err := <-got; err != nil {
		t.Fatalf("call in flight: %v", err)
	}
	select {
	case err := <-s.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return")
	}
	s.done <- nil // the cleanup waits for Serve too
}

func TestTLS(t *testing.T) {
	certPEM, keyPEM := selfSigned(t)
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadTLS(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadTLS(certFile, certFile); err == nil {
		t.Fatal("a certificate as the key was accepted")
	}
	s := serve(t, &fakeAPI{}, cfg)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	conn := dial(t, s.addr, credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: "localhost"}))
	var resp symv1.GetResponse
	if err := conn.Invoke(withToken(t.Context(), testToken), symv1connect.SymServiceGetProcedure, &symv1.GetRequest{}, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.GetInfo().GetUid() != 1 {
		t.Fatalf("get %v", &resp)
	}
	plain := dial(t, s.addr, nil)
	if err := plain.Invoke(withToken(t.Context(), testToken), symv1connect.SymServiceGetProcedure, &symv1.GetRequest{}, &resp); err == nil {
		t.Fatal("h2c was accepted on a TLS listener")
	}
}

func selfSigned(t *testing.T) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func TestParseTokens(t *testing.T) {
	bad := map[string]string{
		"empty":        "\n# only a comment\n",
		"one field":    "pms\n",
		"three fields": "pms sekrit extra\n",
		"name twice":   "pms sekrit\npms other\n",
		"token twice":  "pms sekrit\nops sekrit\n",
	}
	for name, in := range bad {
		_, err := ParseTokens(strings.NewReader(in))
		if err == nil {
			t.Fatalf("%s was accepted", name)
		}
		if strings.Contains(err.Error(), "sekrit") {
			t.Fatalf("%s: the error names the token: %v", name, err)
		}
	}
	tokens, err := ParseTokens(strings.NewReader("  pms   tok1  \nops\ttok2\n"))
	if err != nil {
		t.Fatal(err)
	}
	if name, ok := tokens.match("tok2"); !ok || name != "ops" {
		t.Fatalf("tok2 -> %q %v", name, ok)
	}
	if _, ok := tokens.match("tok3"); ok {
		t.Fatal("unknown token matched")
	}
	if _, err := LoadTokens(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("a missing file was accepted")
	}
	if _, err := New(&fakeAPI{}, Config{}); err == nil {
		t.Fatal("a server without tokens was built")
	}
}

func TestWatchEnd(t *testing.T) {
	err := watchEnd(t.Context(), sym.ErrSlowWatcher)
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("slow: %v", err)
	}
	err = watchEnd(t.Context(), &sym.Error{Code: sym.CodeUnavailable, Reason: "change stream stopped"})
	var ce *connect.Error
	if !errors.As(err, &ce) || ce.Code() != connect.CodeUnavailable || ce.Message() != "unavailable: change stream stopped" {
		t.Fatalf("stopped: %v", err)
	}
	gone, cancel := context.WithCancel(t.Context())
	cancel()
	if err := watchEnd(gone, gone.Err()); connect.CodeOf(err) != connect.CodeCanceled || !errors.Is(err, context.Canceled) {
		t.Fatalf("client gone: %v", err)
	}
}
