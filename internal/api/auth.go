package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"mime"
	"net/http"
	"strings"
	"time"

	apiv1 "github.com/BullionBear/mftiko/contract/gen/api/v1"
)

// Cookie and bearer authentication (E10-D009, E10-D050, E10-D051).
const (
	// cookieName is the console session cookie (OpenAPI cookieAuth).
	cookieName = "mftiko_session"
	// cookieTTL is the lifetime of a login cookie (E10-D009).
	cookieTTL = 12 * time.Hour
	// headerRequestID is the response header carrying the server-generated id (E10-D053).
	headerRequestID = "X-Request-Id"
)

// sessionClaims is the cookie payload. HMAC-SHA256 covers the JSON bytes; the cookie value is
// base64url(JSON) + "." + base64url(mac), with no padding. exp is unix seconds.
type sessionClaims struct {
	Name string `json:"name"`
	Exp  int64  `json:"exp"`
}

// identity is who this request authenticated as. expires is set for a cookie and zero for a bearer
// token, which has no expiry of its own (E10-D009).
type identity struct {
	name    string
	expires time.Time
	cookie  bool
}

func (s *Server) clock() time.Time {
	if s != nil && s.now != nil {
		return s.now()
	}
	return time.Now()
}

func isLogin(r *http.Request) bool {
	return r.Method == http.MethodPost && requestPath(r) == "/api/v1/auth/login"
}

func isWrite(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

// bearerAuthed reports whether the request was authenticated with a bearer token. Login and cookie
// requests are not (E10-D110).
func bearerAuthed(st *accessState) bool {
	return st.actor != nil && !st.actor.cookie
}

func jsonContentType(r *http.Request) bool {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && media == "application/json"
}

// bearer reports whether an Authorization Bearer credential is present (E10-D051). An empty token is
// still present: it is an invalid bearer, not an invitation to try the cookie. A value of "Bearer"
// with no token is the same, including when the transport drops the trailing space.
func bearer(r *http.Request) (string, bool) {
	h := strings.TrimSpace(r.Header.Get("Authorization"))
	if h == "" {
		return "", false
	}
	scheme, rest, found := strings.Cut(h, " ")
	if !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	if !found {
		return "", true
	}
	return strings.TrimSpace(rest), true
}

// userForToken hashes token and compares it with every configured digest. It always scans the
// whole list, so which user matched does not decide how long the compare runs.
func (s *Server) userForToken(token string) (User, bool) {
	sum := sha256.Sum256([]byte(token))
	var found User
	ok := false
	for _, u := range s.cfg.Users {
		if subtle.ConstantTimeCompare(sum[:], u.Token[:]) == 1 {
			found = u
			ok = true
		}
	}
	return found, ok
}

func (s *Server) knownUser(name string) bool {
	for _, u := range s.cfg.Users {
		if u.Name == name {
			return true
		}
	}
	return false
}

func signCookie(key, name string, exp time.Time) (string, error) {
	payload, err := json.Marshal(sessionClaims{Name: name, Exp: exp.Unix()})
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write(payload)
	sum := mac.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sum), nil
}

func openCookie(key, value string) (string, time.Time, bool) {
	encPayload, encMAC, ok := strings.Cut(value, ".")
	if !ok || encPayload == "" || encMAC == "" || strings.Contains(encMAC, ".") {
		return "", time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(encPayload)
	if err != nil {
		return "", time.Time{}, false
	}
	got, err := base64.RawURLEncoding.DecodeString(encMAC)
	if err != nil {
		return "", time.Time{}, false
	}
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write(payload)
	if !hmac.Equal(got, mac.Sum(nil)) {
		return "", time.Time{}, false
	}
	var claims sessionClaims
	if json.Unmarshal(payload, &claims) != nil || claims.Name == "" {
		return "", time.Time{}, false
	}
	return claims.Name, time.Unix(claims.Exp, 0).UTC(), true
}

func (s *Server) verifyCookie(value string) (string, time.Time, bool) {
	name, exp, ok := openCookie(s.cfg.CookieKey, value)
	if !ok || !s.clock().Before(exp) || !s.knownUser(name) {
		return "", time.Time{}, false
	}
	return name, exp, true
}

func setSessionCookie(w http.ResponseWriter, value string, exp, now time.Time) {
	maxAge := int(exp.Sub(now).Seconds())
	if maxAge < 0 {
		maxAge = 0
	}
	http.SetCookie(w, &http.Cookie{
		Name:     cookieName,
		Value:    value,
		Path:     "/",
		Expires:  exp.UTC(),
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   true, // always, including plaintext dev HTTP (E10-D050)
		SameSite: http.SameSiteStrictMode,
	})
}

func clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     cookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
	})
}

// authenticate accepts a bearer token or, when no bearer is present, the session cookie (E10-D051).
// A bearer that does not match a user is 401 even if the cookie would have.
func (s *Server) authenticate(w http.ResponseWriter, r *http.Request, st *accessState) bool {
	if token, ok := bearer(r); ok {
		user, matched := s.userForToken(token)
		if !matched {
			writeProblem(w, http.StatusUnauthorized, apiv1.ErrorCodeUnauthenticated, http.StatusText(http.StatusUnauthorized), "invalid bearer token", nil)
			return false
		}
		st.actor = &identity{name: user.Name}
		return true
	}
	c, err := r.Cookie(cookieName)
	if err != nil || c.Value == "" {
		writeProblem(w, http.StatusUnauthorized, apiv1.ErrorCodeUnauthenticated, http.StatusText(http.StatusUnauthorized), "authentication required", nil)
		return false
	}
	name, exp, ok := s.verifyCookie(c.Value)
	if !ok {
		writeProblem(w, http.StatusUnauthorized, apiv1.ErrorCodeUnauthenticated, http.StatusText(http.StatusUnauthorized), "invalid cookie", nil)
		return false
	}
	st.actor = &identity{name: name, expires: exp, cookie: true}
	return true
}

// Login exchanges a token for the session cookie (E10-D009). The token is not logged and is removed
// from the audit body before the row is queued.
func (s *Server) Login(w http.ResponseWriter, r *http.Request) {
	st := accessFrom(r.Context())
	raw := []byte(nil)
	if st != nil {
		raw = st.body
	}
	var req apiv1.LoginRequest
	if json.Unmarshal(raw, &req) != nil {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "request body is not JSON", nil)
		return
	}
	if req.Token == nil || *req.Token == "" {
		writeProblem(w, http.StatusBadRequest, apiv1.ErrorCodeValidationFailed, http.StatusText(http.StatusBadRequest), "token is required", []apiv1.FieldError{{
			Path:   "/token",
			Reason: "required",
		}})
		return
	}
	user, ok := s.userForToken(*req.Token)
	if !ok {
		writeProblem(w, http.StatusUnauthorized, apiv1.ErrorCodeUnauthenticated, http.StatusText(http.StatusUnauthorized), "invalid token", nil)
		return
	}
	now := s.clock()
	exp := now.Add(cookieTTL)
	value, err := signCookie(s.cfg.CookieKey, user.Name, exp)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, apiv1.ErrorCodeInternal, http.StatusText(http.StatusInternalServerError), "internal error", nil)
		return
	}
	setSessionCookie(w, value, exp, now)
	if st != nil {
		st.actor = &identity{name: user.Name, expires: exp, cookie: true}
	}
	w.WriteHeader(http.StatusNoContent)
}

// Logout clears the session cookie (E10-D009). The value is stateless, so clearing it is what the
// browser stores; a copy the client keeps remains valid until exp.
func (s *Server) Logout(w http.ResponseWriter, _ *http.Request) {
	clearSessionCookie(w)
	w.WriteHeader(http.StatusNoContent)
}

// GetMe returns the authenticated user. expires_at is the cookie expiry, and null for a bearer
// token (the generated Me type has no omitempty on that field).
func (s *Server) GetMe(w http.ResponseWriter, r *http.Request) {
	st := accessFrom(r.Context())
	if st == nil || st.actor == nil {
		writeProblem(w, http.StatusUnauthorized, apiv1.ErrorCodeUnauthenticated, http.StatusText(http.StatusUnauthorized), "authentication required", nil)
		return
	}
	me := apiv1.Me{Name: st.actor.name}
	if st.actor.cookie {
		t := st.actor.expires.UTC()
		me.ExpiresAt = &t
	}
	writeJSON(w, http.StatusOK, me)
}
