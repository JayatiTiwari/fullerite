package collector

import (
	"fullerite/config"
	"fullerite/dropwizard"
	"fullerite/metric"
	"fullerite/util"

	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	l "github.com/Sirupsen/logrus"
)

const (
	// MetricTypeCounter String for counter metric type
	MetricTypeCounter string = "COUNTER"
	// MetricTypeGauge String for Gauge metric type
	MetricTypeGauge string = "GAUGE"
)

type nerveUWSGICollector struct {
	baseCollector

	configFilePath        string
	queryPath             string
	timeout               int
	servicesWhitelist     []string
	serviceHeadersMap     map[string]map[string]string
	workersStatsEnabled   bool
	workersStatsQueryPath string
	workersStatsBlacklist []string
}

func init() {
	RegisterCollector("NerveUWSGI", newNerveUWSGI)
}

// Default values of configuration fields
func newNerveUWSGI(channel chan metric.Metric, initialInterval int, log *l.Entry) Collector {
	col := new(nerveUWSGICollector)

	col.log = log
	col.channel = channel
	col.interval = initialInterval

	col.name = "NerveUWSGI"
	col.configFilePath = "/etc/nerve/nerve.conf.json"
	col.queryPath = "status/metrics"
	col.workersStatsQueryPath = "status/uwsgi"
	col.timeout = 2

	return col
}

// Rewrites config variables from the global config
func (n *nerveUWSGICollector) Configure(configMap map[string]interface{}) {
	if val, exists := configMap["queryPath"]; exists {
		n.queryPath = val.(string)
	}
	if val, exists := configMap["configFilePath"]; exists {
		n.configFilePath = val.(string)
	}
	if val, exists := configMap["servicesWhitelist"]; exists {
		n.servicesWhitelist = config.GetAsSlice(val)
	}
	if val, exists := configMap["serviceHeaders"]; exists {
		temp := val.(map[string]interface{})
		for service, headers := range temp {
			n.serviceHeadersMap[service] = headers.(map[string]string)
		}
	}
	if val, exists := configMap["workersStatsBlacklist"]; exists {
		n.workersStatsBlacklist = config.GetAsSlice(val)
	}
	if val, exists := configMap["workersStatsEnabled"]; exists {
		n.workersStatsEnabled = config.GetAsBool(val, false)
	}
	if val, exists := configMap["workersStatsQueryPath"]; exists {
		n.workersStatsQueryPath = val.(string)
	}
	if val, exists := configMap["http_timeout"]; exists {
		n.timeout = config.GetAsInt(val, 2)
	}

	n.configureCommonParams(configMap)
}

// Parses nerve config from HTTP uWSGI stats endpoints
func (n *nerveUWSGICollector) Collect() {
	rawFileContents, err := ioutil.ReadFile(n.configFilePath)
	if err != nil {
		n.log.Warn("Failed to read the contents of file ", n.configFilePath, " because ", err)
		return
	}

	services, err := util.ParseNerveConfig(&rawFileContents, false)
	if err != nil {
		n.log.Warn("Failed to parse the nerve config at ", n.configFilePath, ": ", err)
		return
	}
	n.log.Debug("Finished parsing Nerve config into ", services)

	for _, service := range services {
		go n.queryService(service.Name, service.Host, service.Port)
	}
}

// Fetches and computes stats from metrics HTTP endpoint,
// calls an additional endpoint if UWSGI is detected
func (n *nerveUWSGICollector) queryService(serviceName string, host string, port int) {
	serviceLog := n.log.WithField("service", serviceName)
	endpoint := fmt.Sprintf("http://%s:%d/%s", host, port, n.queryPath)
	serviceLog.Debug("making GET request to ", endpoint)
	headers := make(map[string]string)
	if val, exists := n.serviceHeadersMap[serviceName]; exists {
		headers = val
	}
	serviceLog.Debug("GET request headers ", headers)
	rawResponse, schemaVer, err := queryEndpoint(endpoint, headers, n.timeout)
	if err != nil {
		serviceLog.Warn("Failed to query endpoint ", endpoint, ": ", err)
		return
	}
	metrics, err := dropwizard.Parse(rawResponse, schemaVer, n.serviceInWhitelist(serviceName))
	if err != nil {
		serviceLog.Warn("Failed to parse response into metrics: ", err)
		return
	}
	// If we detect metrics from uwsgi, we try to fetch an additional Workers info
	// If this was a separate collector, there would be no way to figure that out
	// without a costly additional HTTP call.
	// This prevent us from having to maintain a whitelist of services to query
	// or from flooding all non UWSGI services with these requests.
	// We still maintain a blacklist just in case
	if strings.Contains(schemaVer, "uwsgi") && n.workersStatsEnabled && !n.serviceInWorkersStatsBlacklist(serviceName) {
		extraDims := dropwizard.ExtractServiceDims(rawResponse)
		serviceLog.Debug("Trying to fetch workers stats")
		uwsgiWorkerStatsEndpoint := fmt.Sprintf("http://%s:%d/%s", host, port, n.workersStatsQueryPath)
		uwsgiWorkerStatsMetrics, err := n.tryFetchUWSGIWorkersStats(serviceName, uwsgiWorkerStatsEndpoint)
		if err != nil {
			serviceLog.Info("Could not get additional worker stat metrics")
		} else {
			// Add the metrics to our existing ones so we get the post process for free.
			serviceLog.Debug("Additional workers metrics collected: ", len(uwsgiWorkerStatsMetrics))
			metric.AddToAll(&uwsgiWorkerStatsMetrics, extraDims)
			for _, v := range uwsgiWorkerStatsMetrics {
				metrics = append(metrics, v)
			}
		}
	}

	metric.AddToAll(&metrics, map[string]string{
		"service": serviceName,
		"port":    strconv.Itoa(port),
	})
	serviceLog.Debug("Sending ", len(metrics), " to channel")
	for _, m := range metrics {
		if !n.ContainsBlacklistedDimension(m.Dimensions) {
			n.Channel() <- m
		}
	}
}

func queryEndpoint(endpoint string, headers map[string]string, timeout int) ([]byte, string, error) {
	client := http.Client{
		Timeout: time.Duration(timeout) * time.Second,
	}

	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return []byte{}, "", err
	}
	for key, val := range headers {
		// Host is a special header and we cannot use Header.Add for it
		if key == "Host" {
			req.Host = val
		} else {
			req.Header.Add(key, val)
		}
	}

	rsp, err := client.Do(req)
	if rsp != nil {
		defer func() {
			io.Copy(ioutil.Discard, rsp.Body)
			rsp.Body.Close()
		}()
	}

	if err != nil {
		return []byte{}, "", err
	}

	if rsp != nil && rsp.StatusCode != 200 {
		err := fmt.Errorf("%s returned %d error code", endpoint, rsp.StatusCode)
		return []byte{}, "", err
	}

	schemaVer := rsp.Header.Get("Metrics-Schema")
	if schemaVer == "" {
		schemaVer = "default"
	}

	txt, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return []byte{}, "", err
	}

	return txt, schemaVer, nil
}

// serviceInWhitelist returns true if the service name passed as argument
// is found among the ones whitelisted by the user
func (n *nerveUWSGICollector) serviceInWhitelist(service string) bool {
	for _, s := range n.servicesWhitelist {
		if s == service {
			return true
		}
	}
	return false
}

// serviceInWhitelist returns true if the service name passed as argument
// is found among the ones whitelisted by the user
func (n *nerveUWSGICollector) serviceInWorkersStatsBlacklist(service string) bool {
	for _, s := range n.workersStatsBlacklist {
		if s == service {
			return true
		}
	}
	return false
}

// Fetches and computes status stats from an HTTP endpoint
func (n *nerveUWSGICollector) tryFetchUWSGIWorkersStats(serviceName string, endpoint string) ([]metric.Metric, error) {
	emptyResult := []metric.Metric{}
	serviceLog := n.log.WithField("service", serviceName)
	serviceLog.Debug("making GET request to ", endpoint)
	headers := make(map[string]string)
	if val, exists := n.serviceHeadersMap[serviceName]; exists {
		headers = val
	}
	serviceLog.Debug("GET request headers ", headers)
	rawResponse, _, err := queryEndpoint(endpoint, headers, n.timeout)
	if err != nil {
		serviceLog.Info("Failed to query workers stats endpoint ", endpoint, ": ", err)
		return emptyResult, err
	}
	metrics, err := util.ParseUWSGIWorkersStats(rawResponse)
	if err != nil {
		serviceLog.Info("No workers stats retreived: ", err)
		return emptyResult, err
	}
	return metrics, nil
}
