package collector

import (
	"fullerite/config"
	"fullerite/dropwizard"
	"fullerite/metric"

	"fmt"

	l "github.com/Sirupsen/logrus"
)

// The HTTP Dropwizard Collector allows to collect metrics emitted by java/python services
// with one of the schemas defined at dropwizard/base_parser.go#L80.
// User needs to specify port and path where the service'metrics endpoint is setup.
type httpDropwizardCollector struct {
	baseCollector

	endpoints []ServiceEndpoint
	timeout   int
}

// ServiceEndpoint defines a struct for endpoints
type ServiceEndpoint struct {
	// Name is the service name
	Name string
	// Port is the service metrics endpoint port
	Port string
	// Path is the service metrics endpoint path (i.e. status/metrics)
	Path string
}

func init() {
	RegisterCollector("HttpDropwizard", newHTTPDropwizard)
}

func newHTTPDropwizard(channel chan metric.Metric, initialInterval int, log *l.Entry) Collector {
	col := new(httpDropwizardCollector)

	col.log = log
	col.channel = channel
	col.interval = initialInterval

	col.name = "HttpDropwizard"
	col.timeout = 3
	return col
}

func (h *httpDropwizardCollector) Configure(configMap map[string]interface{}) {
	if val, exists := configMap["endpoints"]; exists {
		val := val.([]interface{})
		h.endpoints = make([]ServiceEndpoint, len(val))
		index := 0
		for _, e := range val {
			endpoint := config.GetAsMap(e)
			h.endpoints[index] = ServiceEndpoint{
				Name: endpoint["service_name"],
				Port: endpoint["port"],
				Path: endpoint["path"],
			}
			index++
		}
	}

	if val, exists := configMap["http_timeout"]; exists {
		h.timeout = config.GetAsInt(val, 2)
	}

	h.configureCommonParams(configMap)
}

func (h *httpDropwizardCollector) Collect() {
	for _, endpoint := range h.endpoints {
		go h.queryService(endpoint)
	}
}

func (h *httpDropwizardCollector) queryService(s ServiceEndpoint) {
	serviceLog := h.log.WithField("service", s.Name)

	endpoint := fmt.Sprintf("http://localhost:%s/%s", s.Port, s.Path)
	serviceLog.Debug("making GET request to ", endpoint)

	rawResponse, schemaVer, err := queryEndpoint(endpoint, map[string]string{}, h.timeout)
	if err != nil {
		serviceLog.Warn("Failed to query endpoint ", endpoint, ": ", err)
		return
	}
	metrics, err := dropwizard.Parse(rawResponse, schemaVer, true)
	if err != nil {
		serviceLog.Warn("Failed to parse response into metrics: ", err)
		return
	}

	metric.AddToAll(&metrics, map[string]string{
		"service": s.Name,
		"port":    s.Port,
	})
	serviceLog.Debug("Sending ", len(metrics), " to channel")
	for _, m := range metrics {
		if !h.ContainsBlacklistedDimension(m.Dimensions) {
			h.Channel() <- m
		}
	}
}
