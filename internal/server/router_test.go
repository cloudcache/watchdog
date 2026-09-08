package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRequestBodyLimitRejectsBeforeHandler(t *testing.T) {
	s := &Server{}
	s.engine = s.newRouter()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/session/login", strings.NewReader(strings.Repeat("x", int(maxAPIRequestBytes)+1)))
	rec := httptest.NewRecorder()
	s.engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge || !strings.Contains(rec.Body.String(), `"code":"request_too_large"`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestRequestID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(requestID())
	r.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, c.GetString("request_id"))
	})

	t.Run("preserves valid caller ID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Request-ID", "caller-123")
		res := httptest.NewRecorder()
		r.ServeHTTP(res, req)
		if res.Code != http.StatusOK || res.Header().Get("X-Request-ID") != "caller-123" || res.Body.String() != "caller-123" {
			t.Fatalf("unexpected response: status=%d header=%q body=%q", res.Code, res.Header().Get("X-Request-ID"), res.Body.String())
		}
	})

	t.Run("generates missing ID", func(t *testing.T) {
		res := httptest.NewRecorder()
		r.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/", nil))
		if res.Code != http.StatusOK || res.Header().Get("X-Request-ID") == "" || res.Body.String() != res.Header().Get("X-Request-ID") {
			t.Fatalf("unexpected response: status=%d header=%q body=%q", res.Code, res.Header().Get("X-Request-ID"), res.Body.String())
		}
	})
}

func TestPasswordBoundsAndHash(t *testing.T) {
	for _, password := range []string{"1234567", strings.Repeat("a", maxPasswordBytes+1)} {
		if validPassword(password) {
			t.Fatalf("password length %d must be rejected", len(password))
		}
	}
	password := strings.Repeat("a", minPasswordBytes)
	if !validPassword(password) {
		t.Fatal("minimum-length password rejected")
	}
	hash, err := hashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	if !checkPassword(hash, password) || checkPassword(hash, "different-password") {
		t.Fatal("password verification mismatch")
	}
}
