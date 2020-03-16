package internalserver

import (
	"fullerite/config"
	"fullerite/metric"

	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"

	l "github.com/Sirupsen/logrus"
)

const (
	defaultPort = 19090
)

// InternalServer will collect from each handler the status and return it over HTTP
type InternalServer struct {
	log               *l.Entry
	handlerStatFunc   InternalStatFunc
	collectorStatFunc InternalStatFunc
	port              int
}

// InternalStatFunc can be used to extract metrics
type InternalStatFunc func() (stats map[string]metric.InternalMetrics)

// ResponseFormat is the structure of the response from an http request
type ResponseFormat struct {
	Memory     metric.InternalMetrics
	Handlers   map[string]metric.InternalMetrics
	Collectors map[string]metric.InternalMetrics
}

// New createse a new internal server instance
func New(cfg config.Config, h InternalStatFunc, c InternalStatFunc) *InternalServer {
	srv := new(InternalServer)
	srv.log = l.WithFields(l.Fields{"app": "fullerite", "pkg": "internalserver"})
	srv.handlerStatFunc = h
	srv.collectorStatFunc = c
	srv.configure(cfg.InternalServerConfig)
	return srv
}

// Run starts a server on the specified port
func (srv *InternalServer) Run() {
	srv.log.Info(fmt.Sprintf("Starting to run internal metrics server on port %d", srv.port))
	http.HandleFunc("/metrics", srv.handleInternalMetricsRequest)
	http.HandleFunc("/metrics/prometheus", srv.handlePrometheusMetricsRequest)

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", srv.port))
	if err != nil {
		srv.log.Error("Failed to start internal server: ", err)
	}

	srv.port = ln.Addr().(*net.TCPAddr).Port // reset the port with the bind port number (would change if port 0 is used)

	if http.Serve(ln, nil) != nil {
		srv.log.Error("Failed to start internal server: ", err)
	}
}

func (srv *InternalServer) configure(cfgMap map[string]interface{}) {

	if val, exists := (cfgMap)["port"]; exists {
		srv.port = config.GetAsInt(val, defaultPort)
	} else {
		srv.port = defaultPort
	}
}

// this is what services the request. The response will be JSON formatted like this:
// 	{
// 		"memory": {
// 			"counters": {
//				"TotalAlloc": 43.2,
//				"NumGoRoutine": 12.3
//			},
//			"gauges": {
//				"Alloc": 23.4,
//				"Sys": 12.43
//			}
//		},
//		"handlers": {
//			"somehandler": {
//				"counters": {
//					"totalEmissions": 12332,
//				},
//				"gauges": {
//					"averageEmissionTiming": 1.34,
//				}
//			}
//		}
//	}
//
func (srv InternalServer) handleInternalMetricsRequest(writer http.ResponseWriter, req *http.Request) {
	rspString := string(*srv.buildResponse())

	srv.log.Debug("Finished building response: ", rspString)
	io.WriteString(writer, rspString)
}

func (srv InternalServer) handlePrometheusMetricsRequest(writer http.ResponseWriter, req *http.Request) {
	prometheusInternalMetricsMemoryStats(writer)
	prometheusInternalMetricsHandlerStats(writer, srv.handlerStatFunc())
	prometheusInternalMetricsCollectorStats(writer, srv.collectorStatFunc())
}

func prometheusInternalMetricsMemoryStats(writer http.ResponseWriter) {
	prometheusInternalMetricsEmit("memory", "", writer, getMemoryStats(), true)
}

func prometheusInternalMetricsHandlerStats(writer http.ResponseWriter, stats map[string]metric.InternalMetrics) {
	emitTypes := true
	for hName, h := range stats {
		prometheusInternalMetricsEmit("handler", fmt.Sprintf("handler=\"%s\"", hName), writer, &h, emitTypes)
		emitTypes = false
	}
}

func prometheusInternalMetricsCollectorStats(writer http.ResponseWriter, stats map[string]metric.InternalMetrics) {
	emitTypes := true
	for hName, h := range stats {
		prometheusInternalMetricsEmit("handler", fmt.Sprintf("collector=\"%s\"", hName), writer, &h, emitTypes)
		emitTypes = false
	}
}

func prometheusInternalMetricsEmit(namePrefix string, dimString string, writer http.ResponseWriter, im *metric.InternalMetrics, emitTypes bool) {
	for k, v := range im.Counters {
		metricName := strings.Replace(k, ".", "_", -1)
		if emitTypes {
			io.WriteString(writer, fmt.Sprintf("# TYPE fullerite_internal_%s_%s_count counter\n", namePrefix, metricName))
		}
		io.WriteString(writer, fmt.Sprintf("fullerite_internal_%s_%s_count{%s} %f\n", namePrefix, metricName, dimString, v))
	}
	for k, v := range im.Gauges {
		metricName := strings.Replace(k, ".", "_", -1)
		if emitTypes {
			io.WriteString(writer, fmt.Sprintf("# TYPE fullerite_internal_%s_%s gauge\n", namePrefix, metricName))
		}
		io.WriteString(writer, fmt.Sprintf("fullerite_internal_%s_%s{%s} %f\n", namePrefix, metricName, dimString, v))
	}
}

// responsible for querying each handler and serializing the total response
func (srv InternalServer) buildResponse() *[]byte {
	memoryStats := getMemoryStats()
	rsp := ResponseFormat{}
	rsp.Memory = *memoryStats
	rsp.Handlers = srv.handlerStatFunc()
	rsp.Collectors = srv.collectorStatFunc()
	asString, err := json.Marshal(rsp)
	if err != nil {
		srv.log.Warn("Failed to marshal response ", rsp, " because of error ", err)
	}

	return &asString
}

// gets the actual memory stats
func memoryStats() *runtime.MemStats {
	stats := new(runtime.MemStats)
	runtime.ReadMemStats(stats)
	return stats
}

// converts the memory stats to a map. The response is in the form like this: {counters: [], gauges: []}
func getMemoryStats() *metric.InternalMetrics {
	m := memoryStats()

	counters := map[string]float64{
		"NumGoroutine": float64(runtime.NumGoroutine()),
		"TotalAlloc":   float64(m.TotalAlloc),
		"Lookups":      float64(m.Lookups),
		"Mallocs":      float64(m.Mallocs),
		"Frees":        float64(m.Frees),
		"PauseTotalNs": float64(m.PauseTotalNs),
		"NumGC":        float64(m.NumGC),
	}

	gauges := map[string]float64{
		"Alloc":        float64(m.Alloc),
		"Sys":          float64(m.Sys),
		"HeapAlloc":    float64(m.HeapAlloc),
		"HeapSys":      float64(m.HeapSys),
		"HeapIdle":     float64(m.HeapIdle),
		"HeapInuse":    float64(m.HeapInuse),
		"HeapReleased": float64(m.HeapReleased),
		"HeapObjects":  float64(m.HeapObjects),
		"StackInuse":   float64(m.StackInuse),
		"StackSys":     float64(m.StackSys),
		"MSpanInuse":   float64(m.MSpanInuse),
		"MSpanSys":     float64(m.MSpanSys),
		"MCacheInuse":  float64(m.MCacheInuse),
		"MCacheSys":    float64(m.MCacheSys),
		"BuckHashSys":  float64(m.BuckHashSys),
		"GCSys":        float64(m.GCSys),
		"OtherSys":     float64(m.OtherSys),
		"NextGC":       float64(m.NextGC),
		"LastGC":       float64(m.LastGC),
	}

	rsp := metric.InternalMetrics{
		Counters: counters,
		Gauges:   gauges,
	}
	return &rsp
}
