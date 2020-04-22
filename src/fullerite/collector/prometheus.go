package collector

import (
	"fmt"

	l "github.com/Sirupsen/logrus"

	"fullerite/config"
	"fullerite/metric"
	"fullerite/util"
)

const (
	// Fullerite version
	version = "0.6.86"

	// Prometheus parser can process metrics in `application/openmetrics-text` and `text/plain` formats
	acceptHeader = `application/openmetrics-text; version=0.0.1,text/plain;version=0.0.4;q=0.5,*/*;q=0.1`

	// Default timeout in seconds for scraping
	defaultTimeoutSecs = 5
)

var userAgentHeader = fmt.Sprintf("Fullerite/%s", version)

// Endpoint type.
type Endpoint struct {
	prefix              string
	url                 string
	headers             map[string]string
	httpGetter          util.HTTPGetter
	generatedDimensions map[string]string
	metricsBlacklist    map[string]bool
	metricsWhitelist    map[string]bool
}

// Prometheus collector type.
type Prometheus struct {
	baseCollector
	endpoints []*Endpoint
}

func init() {
	RegisterCollector("Prometheus", newPrometheus)
}

// newPrometheus creates a new Prometheus collector.
func newPrometheus(channel chan metric.Metric, initialInterval int, log *l.Entry) Collector {
	p := new(Prometheus)

	p.log = log
	p.channel = channel
	p.interval = initialInterval

	p.name = "Prometheus"
	return p
}

// Configure takes a dictionary of values with which the handler can configure itself.
func (p *Prometheus) Configure(configMap map[string]interface{}) {
	v, exists := configMap["endpoints"]
	if !exists {
		p.log.Fatal("No endpoints specified in Prometheus config")
	}

	endpoints, ok := v.([]interface{})
	if !ok {
		p.log.Fatal("Invalid format of config entry `endpoints'")
	}

	for _, e := range endpoints {
		endpoint, ok := e.(map[string]interface{})
		if !ok {
			p.log.Fatal("Invalid format of an item in config entry `endpoints`")
		}

		generatedDimensions := map[string]string{}
		if v, exists := endpoint["generated_dimensions"]; exists {
			v2, ok := v.(map[string]interface{})
			if !ok {
				p.log.Fatal("Invalid format of config entry `generated_dimensions'")
			}
			for k, v := range v2 {
				v2, ok := v.(string)
				if !ok {
					p.log.Fatal("Invalid format of config entry `generated_dimensions'")
				}
				generatedDimensions[k] = v2
			}
		}

		var timeout = defaultTimeoutSecs
		if v, exists := endpoint["timeout"]; exists {
			timeout = config.GetAsInt(v, timeout)
		}
		var metricsWhitelist, metricsBlacklist map[string]bool = nil, nil
		if v, exists := endpoint["metrics_whitelist"]; exists {
			metricsWhitelist = config.GetAsSet(v)
		}
		if v, exists := endpoint["metrics_blacklist"]; exists {
			metricsBlacklist = config.GetAsSet(v)
		}
		httpGetter, err := util.NewHTTPGetter(
			p.getString(endpoint, "serverCaFile"),
			p.getString(endpoint, "clientCertFile"),
			p.getString(endpoint, "clientKeyFile"),
			timeout,
		)
		if err != nil {
			p.log.Fatalf("Error while creating HTTP getter: %+v", err)
		}
		p.endpoints = append(p.endpoints, &Endpoint{
			prefix: endpoint["prefix"].(string),
			url:    p.getRequiredString(endpoint, "url"),
			headers: map[string]string{
				"Accept":                              acceptHeader,
				"User-Agent":                          userAgentHeader,
				"X-Prometheus-Scrape-Timeout-Seconds": fmt.Sprintf("%d", timeout),
			},
			httpGetter:          httpGetter,
			metricsWhitelist:    metricsWhitelist,
			metricsBlacklist:    metricsBlacklist,
			generatedDimensions: generatedDimensions,
		})
	}

	p.configureCommonParams(configMap)
}

func (p *Prometheus) getRequiredString(endpoint map[string]interface{}, name string) string {
	v, exists := endpoint[name]
	if !exists {
		p.log.Fatalf("Invalid format of config entry `%s'", name)
	}
	s, ok := v.(string)
	if !ok {
		p.log.Fatalf("Invalid format of config entry `%s'", name)
	}
	return s
}

func (p *Prometheus) getString(endpoint map[string]interface{}, name string) string {
	v, exists := endpoint[name]
	if !exists {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		p.log.Fatalf("Invalid format of config entry `%s'", name)
	}
	return s
}

// Construct a golang's equivalent of the set data structure
// (`map[string]bool`) from the list of strings.
func (p *Prometheus) getSet(endpoint map[string]interface{}, name string) *map[string]bool {
	var ret *map[string]bool
	if v, exists := endpoint[name]; exists {
		v2, ok := v.([]string)
		if !ok {
			p.log.Fatalf("Invalid format of config entry `%s'", name)
		}
		ret = &map[string]bool{}
		for _, m := range v2 {
			(*ret)[m] = true
		}
	}
	return ret
}

// Collect iterates on all Prometheus endpoints and collect the corresponding metrics
// For each endpoint a gorutine is started to spin up the collection process.
func (p *Prometheus) Collect() {
	for _, endpoint := range p.endpoints {
		go p.collectFromEndpoint(endpoint)
	}
}

// collectFromEndpoint gets metrics from the given endpoint.
func (p *Prometheus) collectFromEndpoint(endpoint *Endpoint) {
	body, contentType, scrapeErr := endpoint.httpGetter.Get(
		endpoint.url,
		endpoint.headers,
	)
	if scrapeErr != nil {
		p.log.Errorf("Error while scraping %s: %s", endpoint.url, scrapeErr)
		return
	}

	metrics, parseErr := util.ExtractPrometheusMetrics(
		body,
		contentType,
		endpoint.metricsWhitelist,
		endpoint.metricsBlacklist,
		endpoint.prefix,
		endpoint.generatedDimensions,
		p.log,
	)
	if parseErr != nil {
		p.log.Errorf("Error while parsing response: %s", parseErr)
		return
	}

	p.sendMetrics(metrics)
}

func (p *Prometheus) sendMetrics(metrics []metric.Metric) {
	for _, m := range metrics {
		p.Channel() <- m
	}
}
