// Command mftiko-webhook-dev is the dev stack's webhook receiver (DECISIONS E16-D021). The dev
// Alertmanager sends every notification to it, and each POST appends one line to -out, which the dev
// compose sets to deploy/dev/data/webhook/received.jsonl (gitignored). E16-04's smoke reads that file.
//
// A line is {"time":"<RFC3339Nano, UTC>","path":"<request path>","body":<request body>}. The body is
// kept as JSON when it is one JSON value, and as a JSON string otherwise. A GET or HEAD answers 200
// and records nothing; it is the receiver's own healthcheck.
//
// -check URL does not serve: it GETs URL and exits 0 on a 2xx. The dev compose runs it as the
// healthcheck of images that have neither a shell nor wget, Loki and Alloy (E16-D026).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/BullionBear/mftiko/internal/platform/buildinfo"
	"github.com/BullionBear/mftiko/pkg/logger"
)

const (
	// maxBody bounds one POST. An Alertmanager notification is a few KiB.
	maxBody = 1 << 20
	// checkTimeout bounds -check, below the compose healthcheck's timeout.
	checkTimeout = 2 * time.Second
)

func main() {
	version := flag.Bool("version", false, "print the release and exit")
	listen := flag.String("listen", ":9099", "address to serve on")
	out := flag.String("out", "deploy/dev/data/webhook/received.jsonl", "file each POST is appended to")
	check := flag.String("check", "", "GET this URL and exit 0 on a 2xx, instead of serving")
	flag.Parse()
	if *version {
		fmt.Println(buildinfo.String())
		return
	}
	if *check != "" {
		if err := probe(*check, checkTimeout); err != nil {
			fmt.Fprintln(os.Stderr, "mftiko-webhook-dev:", err)
			os.Exit(1)
		}
		return
	}
	log := logger.New("mftiko-webhook-dev", buildinfo.Version, buildinfo.CommitHash)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := serve(ctx, *listen, *out, log); err != nil {
		log.Error().Err(err).Msg("exit")
		os.Exit(1)
	}
}

func serve(ctx context.Context, listen, out string, log zerolog.Logger) error {
	srv := &http.Server{
		Addr:              listen,
		Handler:           &receiver{out: out, log: log, now: time.Now},
		ReadHeaderTimeout: 5 * time.Second,
	}
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe() }()
	log.Info().Str("listen", listen).Str("out", out).Msg("serving")
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdown); err != nil {
		return err
	}
	if err := <-done; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// receiver appends one line per POST to out. The file is opened for each line, so deleting it
// between runs starts a new one.
type receiver struct {
	out string
	log zerolog.Logger
	now func() time.Time

	mu sync.Mutex
}

func (rc *receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		w.WriteHeader(http.StatusOK)
		return
	case http.MethodPost:
	default:
		w.Header().Set("Allow", "GET, HEAD, POST")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	line, err := record(rc.now(), r.URL.Path, body)
	if err == nil {
		err = rc.append(line)
	}
	if err != nil {
		rc.log.Error().Err(err).Str("path", r.URL.Path).Msg("not recorded")
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	rc.log.Info().Str("path", r.URL.Path).Int("bytes", len(body)).Msg("received")
	w.WriteHeader(http.StatusOK)
}

func (rc *receiver) append(line []byte) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	f, err := os.OpenFile(rc.out, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(line); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

type entry struct {
	Time string          `json:"time"`
	Path string          `json:"path"`
	Body json.RawMessage `json:"body"`
}

// record is one line of -out, with its newline. A body that is not one JSON value is kept as a string.
func record(at time.Time, path string, body []byte) ([]byte, error) {
	var compact bytes.Buffer
	raw := json.RawMessage(nil)
	if json.Compact(&compact, body) == nil {
		raw = compact.Bytes()
	} else {
		s, err := json.Marshal(string(body))
		if err != nil {
			return nil, err
		}
		raw = s
	}
	line, err := json.Marshal(entry{Time: at.UTC().Format(time.RFC3339Nano), Path: path, Body: raw})
	if err != nil {
		return nil, err
	}
	return append(line, '\n'), nil
}

// probe GETs url and returns nil on a 2xx.
func probe(url string, timeout time.Duration) error {
	client := http.Client{Timeout: timeout}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	return nil
}
