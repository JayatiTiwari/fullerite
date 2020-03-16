package collector

import (
	"testing"

	"encoding/json"
	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"

	"fullerite/metric"
)

func getFakeUWSGIWorkerStatsResponse() []byte {
	return []byte(`{
        "workers":[
		{"status":"idle"},
		{"status":"busy"},
		{"status":"pause"},
		{"status":"cheap"},
		{"status":"sig255"},
		{"status":"invalid"},
		{"status":"idle"},
		{"status":"cheap255"}
	]
	}`)
}

func getFakeHTTPResponse() []byte {
	return []byte(`{
        "utilization": 0.75
	}`)
}

func buildHPAMetrics() *HPAMetrics {
	expectedChan := make(chan metric.Metric)
	var expectedLogger = defaultLog.WithFields(l.Fields{"collector": "fullerite"})
	return newHPAMetrics(expectedChan, 10, expectedLogger).(*HPAMetrics)
}

func TestConfigureHPAMetrics(t *testing.T) {
	d := buildHPAMetrics()
	d.Configure(make(map[string]interface{}))
	assert.Equal(t, d.interval, 10)
	assert.Equal(t, d.name, "HPAMetrics")
	assert.Equal(t, "http://localhost:10255/pods", d.podSpecURL)
	assert.Equal(t, 10, d.kubeletTimeout)
	assert.Equal(t, 10, d.metricsProviderTimeout)
}

func TestSanitizeDimensions(t *testing.T) {
	var dimensions = map[string]string{"paasta.yelp.com/instance": "fake-instance"}
	assert.Equal(t, "fake-instance", sanitizeDimensions(dimensions)["paasta_instance"])
}

func TestParseMetrics(t *testing.T) {
	httpVal, _ := parseHTTPMetrics(getFakeHTTPResponse())
	assert.Equal(t, 0.75, httpVal)
	uwsgiVal, _ := parseUWSGIMetrics(getFakeUWSGIWorkerStatsResponse())
	assert.Equal(t, 0.75, uwsgiVal)
}

func TestAllContainersAreReady(t *testing.T) {
	d := buildHPAMetrics()
	podJSON := []byte(`
	{
		"status": {
			"containerStatuses": [
				{
					"ready": true
				}, {
					"ready": false
				}
			]
		}
	}`)
	var pod1 *corev1.Pod
	json.Unmarshal(podJSON, &pod1)
	assert.False(t, d.allContainersAreReady(pod1))

	podJSON = []byte(`
	{
		"status": {
			"containerStatuses": [
				{
					"ready": true
				}, {
					"ready": true
				}
			]
		}
	}`)
	var pod2 *corev1.Pod
	json.Unmarshal(podJSON, &pod2)
	assert.True(t, d.allContainersAreReady(pod2))
}

func TestGetContainerPort(t *testing.T) {
	podJSON := []byte(`
	{
		"spec": {
			"containers": [
				{
					"name": "fake--instance",
					"ports": [
						{
						  "containerPort": 8888,
						  "protocol": "TCP"
						}
					]
				}, {
					"name": "hacheck", 
					"ports": [
						{
						  "containerPort": 6666,
						  "protocol": "TCP"
						}
					]
				}
			]
		}
	}`)
	var pod *corev1.Pod
	json.Unmarshal(podJSON, &pod)
	port, _ := getContainerPort(pod, "fake_Instance")
	assert.Equal(t, 8888, port)
}
