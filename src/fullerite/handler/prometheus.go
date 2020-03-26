package handler

import (
	"fullerite/metric"

	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	l "github.com/Sirupsen/logrus"
)

// Beging Fullerite handler boilerplate

// Handler type
type Prometheus struct {
	BaseHandler
}

// newPrometheus returns a new handler.
func newPrometheus(
	channel chan metric.Metric,
	initialInterval int,
	initialBufferSize int,
	initialTimeout time.Duration,
	log *l.Entry,
) Handler {

	inst := new(Prometheus)
	inst.name = "Prometheus"

	inst.interval = initialInterval
	inst.maxBufferSize = initialBufferSize
	inst.log = log
	inst.channel = channel
	return inst
}

// Configure accepts the different configuration options for the Prometheus handler
func (h *Prometheus) Configure(configMap map[string]interface{}) {
	h.configureCommonParams(configMap)
	h.PassBeginAndEnd = true
}

// Run runs the handler main loop
func (h *Prometheus) Run() {
	h.run(h.emitMetrics)
}

func (h *Prometheus) emitMetrics(metrics []metric.Metric) bool {
	for _, m := range metrics {
		addPrometheusMetric(m)
	}
	return true
}

func init() {
	RegisterHandler("Prometheus", newPrometheus)
	metricCollectorTable = make(map[string][]metric.Metric)
	metricOutputTypes = make(map[string]bool)
	metricOutputTable = make(map[string]string)
}

// We maintain 2 sets of tables of metrics
// First the 'metricCollectorTable' which holds all the metrics
// from a single collector run. Due to the way that stats arrive
// at the handler they may be interleved, so we seperate them out
// into different arrays. Once a collector finishes (EndCollection)
// we take all the metrics we've seen from that handler and serialize
// them as a text blob that we store in metricOutputTable.
// This two level approach means that any clients will see a consistent
// set of metrics for each collector run, and the metrics from
// each collector are atomically replaced as far as clients see.
var metricCollectorTableMutex = &sync.Mutex{}
var metricCollectorTable map[string][]metric.Metric
var metricOutputTableMutex = &sync.Mutex{}
var metricOutputTypes map[string]bool // True is counter, false a gauge
var metricOutputTable map[string]string

// addPrometheusMetric is the entrypoint of metrics from the 'normal' collector machinery
func addPrometheusMetric(m metric.Metric) {
	collectorName, _ := m.GetDimensionValue("collector")
	metricCollectorTableMutex.Lock()
	if m.BeginCollection() {
		if _, ok := metricCollectorTable[collectorName]; !ok {
			metricCollectorTable[collectorName] = make([]metric.Metric, 0)
		}
		metricCollectorTableMutex.Unlock()
		return
	}
	if m.EndCollection() {
		collected := metricCollectorTable[collectorName]
		metricCollectorTable[collectorName] = make([]metric.Metric, 0)
		metricCollectorTableMutex.Unlock()
		// we copied the array above into a local variable 'collected'
		// therefore we can safely call writeTable asynchronously
		// rather than having to potenttially wait on the output table
		// mutex here
		go writeTable(collectorName, collected)
		return
	}
	metricCollectorTable[collectorName] = append(metricCollectorTable[collectorName], m)
	if m.MetricType == metric.Gauge {
		metricOutputTypes[m.Name] = false
	} else {
		metricOutputTypes[m.Name] = true
	}
	metricCollectorTableMutex.Unlock()
}

// Serialize a list of metrics into text table
func writeTable(name string, metrics []metric.Metric) {
	var out string
	time := time.Now().UnixNano() / 1000000
	for i, _ := range metrics {
		key := getMetricKey(metrics[i])
		// https://prometheus.io/docs/concepts/data_model/
		// It must match the regex [a-zA-Z_:][a-zA-Z0-9_:]*
		if !nameMatcher.MatchString(key) {
			continue
		} else {
			out += fmt.Sprintf("%s %f %d\n", key, metrics[i].Value, time)
		}
	}
	metricOutputTableMutex.Lock()
	metricOutputTable[name] = out
	metricOutputTableMutex.Unlock()
}

// Entry point from internal metric server, this function dumps the types and text output tables
// to a writer.
func PrometheustableRead(w io.Writer) {
	// Explicitly buffer the output in a string so we have the lock for the minimum time
	// Get out of the critical section before writing the table, which could blok given a slow client
	// We can assume clients will generally be well behaved, so in practice this buffering is on
	// the stack (so cheap) and the concurrency should be fairly limited (only a handful of clients, every 5s)
	var out string
	metricOutputTableMutex.Lock()
	// Write out all of the type info for prometheus metrics
	for k, t := range metricOutputTypes {
		name := lowerFirst(nameEscaper.Replace(k))
		// Throw away non matching types
		if nameMatcher.MatchString(k) {
			typeString := "counter"
			if !t {
				typeString = "gauge"
			}
			out += fmt.Sprintf("# TYPE %s %s\n", name, typeString)
		} else {
			defaultLog.WithFields(l.Fields{"handler": "prometheus"}).Errorf("Non prometheus compatible metric name stored: '%s'", name)
		}
	}
	// Serialize the body
	for _, chunk := range metricOutputTable {
		out += chunk
	}
	metricOutputTableMutex.Unlock()
	w.Write([]byte(out))
}

// Helper functions to do string formatting
var (
	labelEscaper = strings.NewReplacer("\\", `\\`, "\n", `\n`, "\"", `\"`)
	// This is not escaping all possible wrong charaters, just those we've actually observed from collectors
	nameEscaper = strings.NewReplacer("$", "", ".", "_", "-", "_", "/", "", "'", "", " ", "_")
	// https://prometheus.io/docs/concepts/data_model/
	// It must match the regex [a-zA-Z_:][a-zA-Z0-9_:]*
	nameMatcher = regexp.MustCompile("[a-zA-Z_:][a-zA-Z0-9_:]*")
)

func getMetricKey(metric metric.Metric) string {
	var output = lowerFirst(nameEscaper.Replace(metric.Name))
	output += dimensionsToString(metric.Dimensions)
	return output
}

func dimensionsToString(
	dims map[string]string,
) string {
	if len(dims) == 0 {
		return "{}"
	}
	var (
		output    string
		seperator = "{"
	)
	for k, v := range dims {
		output += seperator
		output += k
		output += `="`
		output += labelEscaper.Replace(v)
		output += `"`

		seperator = ","
	}
	output += "}"
	return output
}

func lowerFirst(s string) string {
	if s == "" {
		return ""
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToLower(r)) + s[n:]
}
