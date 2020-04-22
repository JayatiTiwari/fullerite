package handler

import (
	"fullerite/metric"
	"sort"
	"strconv"
	"sync"

	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	l "github.com/Sirupsen/logrus"
)

// collectors is our global list of collectors
var collectorsMutex sync.RWMutex
var collectors Collectors

//Collectors is a map of Collector objects
type Collectors map[string]*Collector

//For egress purposes, we do not want to be blocked by any metrics which are being scraped.
var outBytesMutex sync.Mutex
var outBytes map[string][]byte

//Collector contains all of the metrics and the types for those metrics
type Collector struct {
	Name      string
	Types     map[string]*PrometheusType
	typeMutex sync.Mutex
}

//NewCollector creates a new collector based on its name
func NewCollector(name string) *Collector {
	t := make(map[string]*PrometheusType)
	c := Collector{
		Name:      name,
		Types:     t,
		typeMutex: sync.Mutex{},
	}

	return &c
}

//StoreMetric adds the metric in the list and create the type if it does not already exist.
func (c *Collector) StoreMetric(metricName string, labels string, metricType string, pm PrometheusMetric) {
	key := metricName + labels
	c.typeMutex.Lock()
	t, ok := c.Types[metricName]
	if !ok {
		t = NewPrometheusType(metricName, metricType)
		c.Types[metricName] = t
	}
	t.metricMutex.Lock()
	t.Metrics[key] = pm
	t.metricMutex.Unlock()
	c.typeMutex.Unlock()
}

//GenerateBytesAndDelete creates the list of bytes that represents
//the metric output for this collector, and deletes any metrics or types from before the collector started
func (c *Collector) GenerateBytesAndDelete() {
	collectorsMutex.Lock()
	metricBytes := make([]byte, 0)
	c.typeMutex.Lock()
	for _, t := range c.Types {
		t.metricMutex.Lock()
		metricBytes = append(metricBytes, []byte(t.String())...)

		for key, metric := range t.Metrics {
			v := strconv.FormatFloat(metric.Value, 'f', -1, 64)
			out := fmt.Sprintf("%s %s %d\n", key, v, metric.LastUpdate.Unix())
			metricBytes = append(metricBytes, []byte(out)...)
		}
		t.metricMutex.Unlock()
	}
	c.Types = make(map[string]*PrometheusType)
	c.typeMutex.Unlock()
	collectorsMutex.Unlock()

	outBytesMutex.Lock()
	outBytes[c.Name] = metricBytes
	outBytesMutex.Unlock()
}

//PrometheusType is the type of metric
type PrometheusType struct {
	Name        string
	Type        string
	Metrics     map[string]PrometheusMetric
	metricMutex sync.Mutex
}

//NewPrometheusType generates a new MetricType object based on name and type, and returns a pointer
func NewPrometheusType(name, metricType string) *PrometheusType {
	t := strings.ToLower(metricType)
	if t == "cumcounter" {
		t = "counter"
	}
	m := make(map[string]PrometheusMetric)
	mt := PrometheusType{
		Name:        name,
		Type:        t,
		Metrics:     m,
		metricMutex: sync.Mutex{},
	}

	return &mt
}

func (t *PrometheusType) String() string {
	s := fmt.Sprintf("# TYPE %s %s\n", t.Name, t.Type)
	return s
}

// PrometheusMetric stores prometheus metric details
type PrometheusMetric struct {
	Value      float64
	LastUpdate time.Time
}

// NewPrometheusMetric returns a new prometheus metric object
func NewPrometheusMetric(m metric.Metric) PrometheusMetric {
	pm := PrometheusMetric{
		Value:      m.Value,
		LastUpdate: time.Now(),
	}
	return pm
}

// Prometheus handler type
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
	collectors = make(Collectors)
	outBytes = make(map[string][]byte)
}

// addPrometheusMetric is the entrypoint of metrics from the 'normal' collector machinery
func addPrometheusMetric(m metric.Metric) {
	//Get the name of the collector and check to see if it already in our map
	//if not, create it.
	collectorName, _ := m.GetDimensionValue("collector")
	var c *Collector
	var ok bool
	collectorsMutex.Lock()
	if c, ok = collectors[collectorName]; !ok {
		c = NewCollector(collectorName)
		collectors[collectorName] = c
	}
	collectorsMutex.Unlock()

	if m.BeginCollection() {
		return
	}

	if m.EndCollection() {
		c.GenerateBytesAndDelete()
		return
	}

	name := getMetricKey(m.Name)
	if nameMatcher.MatchString(name) {
		labels := dimensionsToString(m.Dimensions)
		pm := NewPrometheusMetric(m)
		t := m.MetricType
		c.StoreMetric(name, labels, t, pm)
	} else {
        defaultLog.WithFields(l.Fields{"handler": "prometheus"}).Errorf("Non prometheus compatible metric name encountered: '%s'", name)
    }
}

// PrometheustableRead is the ntry point from internal metric server, this function dumps the types and text output
// tables to a writer.
func PrometheustableRead(w io.Writer) {
	out := make([]byte, 0)
	// Write out all of the type info for prometheus metrics
	outBytesMutex.Lock()
	for _, b := range outBytes {
		out = append(out, b...)
	}
	outBytesMutex.Unlock()

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

func getMetricKey(name string) string {
	var output = lowerFirst(nameEscaper.Replace(name))
	return output
}

func dimensionsToString(
	dims map[string]string,
) string {
	if len(dims) == 0 {
		return "{}"
	}

	keys := make([]string, 0, len(dims))
	for key := range dims {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	var (
		output    string
		seperator = "{"
	)
	for _, k := range keys {
		v := dims[k]
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
