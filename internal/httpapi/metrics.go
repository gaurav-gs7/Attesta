package httpapi

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// serverMetrics are the API's Prometheus metrics, in a registry of the
// server's own.
//
// Every label value comes from a fixed set: the route is the pattern the
// request matched (or "other"), the method one of the standard methods (or
// "other"), and the status a three-digit code. A client cannot create new
// series by choosing paths, IDs or methods.
type serverMetrics struct {
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	// archiveRefused counts records the decision archive refused: a fork,
	// a gap or a bad link. Alert on any increase.
	archiveRefused prometheus.Counter
}

func newServerMetrics(startedAt time.Time) *serverMetrics {
	m := &serverMetrics{
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "attesta_http_requests_total",
			Help: "Attesta API requests by route pattern, method and status.",
		}, []string{"route", "method", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "attesta_http_request_duration_seconds",
			Help:    "Attesta API request duration by route pattern and method.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method"}),
		archiveRefused: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "attesta_decision_archive_refused_total",
			Help: "Decision records the archive refused because they fork or break a guard's archived chain.",
		}),
	}
	m.registry.MustRegister(m.requests, m.duration, m.archiveRefused,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "attesta_uptime_seconds",
			Help: "Attesta API uptime in seconds.",
		}, func() float64 { return time.Since(startedAt).Seconds() }),
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

func (m *serverMetrics) handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry})
}

func (s *Server) metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		route, method := routeOf(r), methodOf(r.Method)
		s.metrics.requests.WithLabelValues(route, method, strconv.Itoa(recorder.status)).Inc()
		s.metrics.duration.WithLabelValues(route, method).Observe(time.Since(start).Seconds())
	})
}

// routeOf returns the path pattern the request matched, such as
// /api/v1/guards/{namespace}/{name}, or "other" when nothing matched
// (a 404, or a request rejected before routing).
func routeOf(r *http.Request) string {
	pattern := r.Pattern
	if info := infoFrom(r.Context()); info != nil && info.pattern != "" {
		pattern = info.pattern
	}
	if pattern == "" {
		return "other"
	}
	if _, path, ok := strings.Cut(pattern, " "); ok {
		return path
	}
	return pattern
}

func methodOf(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodOptions:
		return method
	}
	return "other"
}
