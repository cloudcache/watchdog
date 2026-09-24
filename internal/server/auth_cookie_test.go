package server

import (
	"crypto/tls"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRequestIsSecure(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		proto      string
		tls        bool
		want       bool
	}{
		{name: "direct TLS", remoteAddr: "203.0.113.10:1234", tls: true, want: true},
		{name: "loopback TLS proxy", remoteAddr: "127.0.0.1:1234", proto: "https", want: true},
		{name: "IPv6 loopback TLS proxy", remoteAddr: "[::1]:1234", proto: "HTTPS", want: true},
		{name: "loopback plain proxy", remoteAddr: "127.0.0.1:1234", proto: "http", want: false},
		{name: "forged remote header", remoteAddr: "203.0.113.10:1234", proto: "https", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest("GET", "http://watchdog.test/", nil)
			request.RemoteAddr = tt.remoteAddr
			request.Header.Set("X-Forwarded-Proto", tt.proto)
			if tt.tls {
				request.TLS = &tls.ConnectionState{}
			}
			if got := requestIsSecure(request); got != tt.want {
				t.Fatalf("requestIsSecure()=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestAuthCookiesAreSecureBehindLoopbackTLSProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest("POST", "http://watchdog.test/api/v1/session/login", nil)
	context.Request.RemoteAddr = "127.0.0.1:1234"
	context.Request.Header.Set("X-Forwarded-Proto", "https")

	(&Server{}).setAuthCookies(context, "session-token", "csrf-token")
	cookies := recorder.Result().Cookies()
	if len(cookies) != 2 {
		t.Fatalf("cookies=%d, want 2", len(cookies))
	}
	for _, cookie := range cookies {
		if !cookie.Secure {
			t.Errorf("cookie %s is missing Secure", cookie.Name)
		}
	}
}

func TestClearedAuthCookiesStaySecureBehindLoopbackTLSProxy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest("POST", "http://watchdog.test/api/v1/session/logout", nil)
	context.Request.RemoteAddr = "127.0.0.1:1234"
	context.Request.Header.Set("X-Forwarded-Proto", "https")

	(&Server{}).clearAuthCookies(context)
	cookies := recorder.Result().Cookies()
	if len(cookies) != 2 {
		t.Fatalf("cookies=%d, want 2", len(cookies))
	}
	for _, cookie := range cookies {
		if !cookie.Secure {
			t.Errorf("cookie %s is missing Secure", cookie.Name)
		}
		if cookie.MaxAge >= 0 {
			t.Errorf("cookie %s max-age=%d, want deletion", cookie.Name, cookie.MaxAge)
		}
	}
}
