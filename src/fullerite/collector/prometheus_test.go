package collector

import (
	"encoding/json"
	"testing"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"fullerite/metric"
	"fullerite/util"
)

func TestPrometheusNewPrometheus(t *testing.T) {
	expectedChan := make(chan metric.Metric)
	var expectedLogger = defaultLog.WithFields(l.Fields{"collector": "fullerite"})

	p := newPrometheus(expectedChan, 10, expectedLogger).(*Prometheus)

	assert.Equal(t, p.log, expectedLogger)
	assert.Equal(t, p.channel, expectedChan)
	assert.Equal(t, p.interval, 10)
	assert.Equal(t, p.name, "Prometheus")
}

func TestHTTPPrometheusConfigure(t *testing.T) {
	expectedChan := make(chan metric.Metric)
	var expectedLogger = defaultLog.WithFields(l.Fields{"collector": "fullerite"})

	p := newPrometheus(expectedChan, 10, expectedLogger).(*Prometheus)

	testConfig := []byte(`
	{
		"endpoints": [
			{
				"prefix":         "123/",
				"url":            "https://etcd1.nowhere.com:2379/metrics",
				"metrics_whitelist": ["123", "456"],
				"metrics_blacklist": ["78"],
				"generated_dimensions": {
					"foo": "bar"
				}
			}
		]
	}`)
	testConfigMap := make(map[string]interface{})
	json.Unmarshal(testConfig, &testConfigMap)
	p.Configure(testConfigMap)

	var endpoint *Endpoint = p.endpoints[0]

	assert.Equal(t, endpoint.prefix, "123/")
	assert.Equal(t, endpoint.url, "https://etcd1.nowhere.com:2379/metrics")
	assert.Equal(t, endpoint.metricsWhitelist, map[string]bool{"123": true, "456": true})
	assert.Equal(t, endpoint.metricsBlacklist, map[string]bool{"78": true})
	assert.Equal(t, endpoint.generatedDimensions, map[string]string{"foo": "bar"})
	assert.Empty(t, endpoint.grpcGetter)
	assert.Implements(t, (*util.HTTPGetter)(nil), endpoint.httpGetter)
}

func TestGRPCPrometheusConfigure(t *testing.T) {
	expectedChan := make(chan metric.Metric)
	var expectedLogger = defaultLog.WithFields(l.Fields{"collector": "fullerite"})

	p := newPrometheus(expectedChan, 10, expectedLogger).(*Prometheus)

	testConfig := []byte(`
	{
		"endpoints": [
			{
				"prefix":         "123/",
				"url":            "https://etcd1.nowhere.com:2379/metrics",
				"metrics_whitelist": ["123", "456"],
				"metrics_blacklist": ["78"],
				"generated_dimensions": {
					"foo": "bar"
				},
				"isGrpc": true
			}
		]
	}`)
	testConfigMap := make(map[string]interface{})
	json.Unmarshal(testConfig, &testConfigMap)
	p.Configure(testConfigMap)

	var endpoint *Endpoint = p.endpoints[0]

	assert.Equal(t, endpoint.prefix, "123/")
	assert.Equal(t, endpoint.metricsWhitelist, map[string]bool{"123": true, "456": true})
	assert.Equal(t, endpoint.metricsBlacklist, map[string]bool{"78": true})
	assert.Equal(t, endpoint.generatedDimensions, map[string]string{"foo": "bar"})
	assert.Empty(t, endpoint.httpGetter)
	assert.Empty(t, endpoint.headers)
	assert.Implements(t, (*util.GRPCGetter)(nil), endpoint.grpcGetter)
}
