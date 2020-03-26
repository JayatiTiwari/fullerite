package metric

// The different types of metrics that are supported
const (
	Gauge             = "gauge"
	Counter           = "counter"
	CumulativeCounter = "cumcounter"
)

// Metric type holds all the information for a single metric data
// point. Metrics are generated in collectors and passed to handlers.
type Metric struct {
	Name       string            `json:"name"`
	MetricType string            `json:"type"`
	Value      float64           `json:"value"`
	Dimensions map[string]string `json:"dimensions"`
}

// New returns a new metric with name. Default metric type is "gauge"
// and timestamp is set to now. Value is initialized to 0.0.
func New(name string) Metric {
	return Metric{
		Name:       name,
		MetricType: "gauge",
		Value:      0.0,
		Dimensions: make(map[string]string),
	}
}

// WithValue returns metric with value of type Gauge
func WithValue(name string, value float64) Metric {
	metric := New(name)
	metric.Value = value
	return metric
}

// Sentinel returns a sentinel metric, which will force
// a flush in handler
func Sentinel() Metric {
	return WithValue("fullerite.emit_now", 0)
}

// Dummy metric used to signal beginning of a collector run
func BeginCollection(collector string) Metric {
	m := WithValue("fullerite.begin_collection", 0)
	m.AddDimension("collector", collector)
	return m
}

// Dummy metric used to signal end of a collector run
func EndCollection(collector string) Metric {
	m := WithValue("fullerite.end_collection", 0)
	m.AddDimension("collector", collector)
	return m
}

// AddDimension adds a new dimension to the Metric.
func (m *Metric) AddDimension(name, value string) {
	if m.Dimensions == nil {
		m.Dimensions = make(map[string]string)
	}
	m.Dimensions[name] = value
}

// RemoveDimension removes a dimension from the Metric.
func (m *Metric) RemoveDimension(name string) {
	delete(m.Dimensions, name)
}

// AddDimensions adds multiple new dimensions to the Metric.
func (m *Metric) AddDimensions(dimensions map[string]string) {
	for k, v := range dimensions {
		m.AddDimension(k, v)
	}
}

// GetDimensions returns the dimensions of a metric merged with defaults. Defaults win.
func (m *Metric) GetDimensions(defaults map[string]string) (dimensions map[string]string) {
	dimensions = make(map[string]string)
	for name, value := range m.Dimensions {
		dimensions[name] = value
	}
	for name, value := range defaults {
		dimensions[name] = value
	}
	return dimensions
}

// GetDimensionValue returns the value of a dimension if it's set.
func (m *Metric) GetDimensionValue(dimension string) (value string, ok bool) {
	value, ok = m.Dimensions[dimension]
	return
}

// ZeroValue is metric zero value
func (m *Metric) ZeroValue() bool {
	return (len(m.Name) == 0) &&
		(len(m.MetricType) == 0) &&
		(m.Value == 0.0) &&
		(len(m.Dimensions) == 0)
}

// Sentinel is a metric value which forces handler to flush
// all buffered metrics
func (m *Metric) Sentinel() bool {
	return (m.Name == "fullerite.emit_now")
}

// Check for special value
func (m *Metric) BeginCollection() bool {
	return (m.Name == "fullerite.begin_collection")
}

// Check for special value
func (m *Metric) EndCollection() bool {
	return (m.Name == "fullerite.end_collection")
}

// AddToAll adds a map of dimensions to a list of metrics
func AddToAll(metrics *[]Metric, dims map[string]string) {
	for _, m := range *metrics {
		for key, value := range dims {
			m.AddDimension(key, value)
		}
	}
}
