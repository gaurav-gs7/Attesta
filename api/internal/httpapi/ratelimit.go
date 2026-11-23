package httpapi

import (
	"context"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/gaurav-gs7/attesta/api/internal/auth"
)

// RateLimits bounds how fast one client can call /api/. PerIP applies
// before authentication, so a flood of requests with bad or missing
// credentials is cut off without a token lookup or TokenReview per
// request. PerPrincipal applies after it, so one caller cannot starve the
// others however many addresses it uses. A zero rate disables that limit.
type RateLimits struct {
	PerIP        Limit
	PerPrincipal Limit
	// TrustForwardedFor keys the per-IP limit on the last address in
	// X-Forwarded-For (the client as seen by the proxy in front of the
	// API) instead of the connection's address. Set it only when every
	// request arrives through such a proxy; otherwise a client chooses its
	// own key.
	TrustForwardedFor bool
}

// Limit is a token bucket: Rate requests per second on average, up to
// Burst at once.
type Limit struct {
	Rate  float64
	Burst int
}

// DefaultRateLimits are generous for people and CI, and stop a single
// client from saturating the store. The attesta-api binary applies them
// unless ATTESTA_RATE_LIMIT_* overrides them; NewServer applies none
// unless given WithRateLimits.
var DefaultRateLimits = RateLimits{
	PerIP:        Limit{Rate: 50, Burst: 100},
	PerPrincipal: Limit{Rate: 20, Burst: 40},
}

// WithRateLimits sets per-client rate limits on /api/.
func WithRateLimits(l RateLimits) Option {
	return func(s *Server) { s.rateLimits = l }
}

// WithRequestTimeout bounds the context of every /api/ request, so store
// and Prometheus calls made for a client that has gone, or for a slow
// dependency, are cancelled. Workflow runs are detached from it and have
// their own timeout.
func WithRequestTimeout(d time.Duration) Option {
	return func(s *Server) { s.requestTimeout = d }
}

// DefaultRequestTimeout is below the API server's 30s WriteTimeout, so the
// handler gives up and answers before the connection is cut.
const DefaultRequestTimeout = 25 * time.Second

func (s *Server) withTimeout(next http.Handler) http.Handler {
	if s.requestTimeout <= 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/") {
			next.ServeHTTP(w, r)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

const (
	// maxLimiterKeys bounds the memory the limiters use. When the table is
	// full, idle buckets are dropped; if none are idle, new clients share
	// one overflow bucket, so a flood of new addresses throttles itself
	// rather than growing the table.
	maxLimiterKeys = 10_000
	limiterIdle    = 10 * time.Minute
)

type limiter struct {
	limit Limit
	now   func() time.Time

	mu       sync.Mutex
	buckets  map[string]*bucket
	overflow *rate.Limiter
}

type bucket struct {
	lim  *rate.Limiter
	seen time.Time
}

func newLimiter(l Limit) *limiter {
	if l.Rate <= 0 {
		return nil
	}
	if l.Burst < 1 {
		l.Burst = int(math.Ceil(l.Rate))
	}
	return &limiter{
		limit:    l,
		now:      time.Now,
		buckets:  map[string]*bucket{},
		overflow: rate.NewLimiter(rate.Limit(l.Rate), l.Burst),
	}
}

// allow takes a token for key. When there is none it returns how long
// until there will be.
func (l *limiter) allow(key string) (bool, time.Duration) {
	now := l.now()
	l.mu.Lock()
	b, ok := l.buckets[key]
	if !ok {
		if len(l.buckets) >= maxLimiterKeys {
			for k, old := range l.buckets {
				if now.Sub(old.seen) > limiterIdle {
					delete(l.buckets, k)
				}
			}
		}
		if len(l.buckets) < maxLimiterKeys {
			b = &bucket{lim: rate.NewLimiter(rate.Limit(l.limit.Rate), l.limit.Burst)}
			l.buckets[key] = b
		}
	}
	lim := l.overflow
	if b != nil {
		b.seen = now
		lim = b.lim
	}
	l.mu.Unlock()

	r := lim.ReserveN(now, 1)
	if delay := r.DelayFrom(now); delay > 0 {
		r.CancelAt(now)
		return false, delay
	}
	return true, 0
}

func (s *Server) clientIP(r *http.Request) string {
	if s.rateLimits.TrustForwardedFor {
		if xff := r.Header.Values("X-Forwarded-For"); len(xff) > 0 {
			hops := strings.Split(xff[len(xff)-1], ",")
			if ip := net.ParseIP(strings.TrimSpace(hops[len(hops)-1])); ip != nil {
				return ip.String()
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// limitIP rate-limits /api/ requests by client address, before auth.
func (s *Server) limitIP(next http.Handler) http.Handler {
	if s.ipLimiter == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			if ok, wait := s.ipLimiter.allow(s.clientIP(r)); !ok {
				tooManyRequests(w, r, wait)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// limitPrincipal rate-limits authenticated requests by caller identity.
func (s *Server) limitPrincipal(next http.Handler) http.Handler {
	if s.principalLimiter == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p, ok := auth.From(r.Context()); ok {
			if ok, wait := s.principalLimiter.allow(p.Via + ":" + p.Name); !ok {
				tooManyRequests(w, r, wait)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func tooManyRequests(w http.ResponseWriter, r *http.Request, wait time.Duration) {
	w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
	writeJSON(w, http.StatusTooManyRequests, map[string]string{
		"error":     "rate limit exceeded; retry after the Retry-After interval",
		"requestId": RequestID(r.Context()),
	})
}
