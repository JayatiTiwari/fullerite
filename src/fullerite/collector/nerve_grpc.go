package collector

import (
	"fullerite/config"
	"fullerite/metric"
	"fullerite/util"
	"strconv"

	"fmt"
	"io/ioutil"

	l "github.com/Sirupsen/logrus"
)

type nerveGRPCCollector struct {
	baseCollector

	servicesWhitelist []string
	configFilePath    string
	timeout           int
}

func init() {
	RegisterCollector("NerveGRPC", newNerveGRPC)
}

// Default values of configuration fields
func newNerveGRPC(channel chan metric.Metric, initialInterval int, log *l.Entry) Collector {
	col := new(nerveGRPCCollector)

	col.log = log
	col.channel = channel
	col.interval = initialInterval

	col.name = "NerveGRPC"
	col.configFilePath = "/etc/nerve/nerve.conf.json"
	col.timeout = 2

	return col
}

// Rewrites config variables from the global config
func (n *nerveGRPCCollector) Configure(configMap map[string]interface{}) {
	if val, exists := configMap["configFilePath"]; exists {
		n.configFilePath = val.(string)
	}
	if val, exists := configMap["servicesWhitelist"]; exists {
		n.servicesWhitelist = config.GetAsSlice(val)
	}
	if val, exists := configMap["http_timeout"]; exists {
		n.timeout = config.GetAsInt(val, 2)
	}

	n.configureCommonParams(configMap)
}

// Parses nerve config from GRPC endpoints
func (n *nerveGRPCCollector) Collect() {
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
		if n.serviceInWhitelist(service) {
			url := fmt.Sprintf("%s:%d", service.Host, service.Port)
			grpcGetter, err := util.NewGRPCGetter(
				url,
				n.timeout,
			)
			if err != nil {
				n.log.Fatalf("Error while creating GRPC getter: %+v", err)
				return
			}
			go n.queryService(service.Name, service.Port, grpcGetter)
		}
	}
}

func (n *nerveGRPCCollector) serviceInWhitelist(service util.NerveService) bool {
	for _, s := range n.servicesWhitelist {
		if s == service.Name+"."+service.Namespace {
			return true
		}
	}
	return false
}

// queryService fetches and computes stats from metrics GRPC endpoint,
func (n *nerveGRPCCollector) queryService(serviceName string, port int, grpcGetter util.GRPCGetter) {
	serviceLog := n.log.WithField("service", serviceName)
	body, contentType, err := grpcGetter.Get()
	if err != nil {
		serviceLog.Warnf("Error while scraping grpc: %s", err)
		return
	}
	metrics, err := util.ExtractPrometheusMetrics(
		body,
		contentType,
		nil,
		nil,
		"",
		nil,
		serviceLog,
	)
	if err != nil {
		serviceLog.Errorf("Error while parsing response: %s", err)
		return
	}
	metric.AddToAll(&metrics, map[string]string{
		"service": serviceName,
		"port":    strconv.Itoa(port),
	})
	serviceLog.Debug("Sending ", len(metrics), " to channel")
	for _, m := range metrics {
		n.Channel() <- m
	}
}
