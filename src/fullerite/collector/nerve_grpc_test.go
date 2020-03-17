package collector

import (
	"fullerite/metric"

	"testing"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var body = []byte(`
# HELP grpc_server_handled_latency_seconds Histogram of response latency (seconds) of gRPC that had been application-level handled by the server.
# TYPE grpc_server_handled_latency_seconds histogram
grpc_server_handled_latency_seconds_bucket{grpc_type="BIDI_STREAMING",grpc_service="grpc.reflection.v1alpha.ServerReflection",grpc_method="ServerReflectionInfo",le="+Inf",} 1.0
grpc_server_handled_latency_seconds_count{grpc_type="BIDI_STREAMING",grpc_service="grpc.reflection.v1alpha.ServerReflection",grpc_method="ServerReflectionInfo",} 1.0
grpc_server_handled_latency_seconds_sum{grpc_type="BIDI_STREAMING",grpc_service="grpc.reflection.v1alpha.ServerReflection",grpc_method="ServerReflectionInfo",} 0.15
# HELP grpc_server_handled_total Total number of RPCs completed on the server, regardless of success or failure.
# TYPE grpc_server_handled_total counter
grpc_server_handled_total{grpc_type="BIDI_STREAMING",grpc_service="grpc.reflection.v1alpha.ServerReflection",grpc_method="ServerReflectionInfo",code="OK",} 3.0
# HELP grpc_server_started_total Total number of RPCs started on the server.
# TYPE grpc_server_started_total counter
grpc_server_started_total{grpc_type="BIDI_STREAMING",grpc_service="grpc.reflection.v1alpha.ServerReflection",grpc_method="ServerReflectionInfo",} 1.0
# HELP grpc_server_msg_received_total Total number of stream messages received from the client.
# TYPE grpc_server_msg_received_total counter
grpc_server_msg_received_total{grpc_type="BIDI_STREAMING",grpc_service="grpc.reflection.v1alpha.ServerReflection",grpc_method="ServerReflectionInfo",} 5.0
# HELP grpc_server_msg_sent_total Total number of stream messages sent by the server.
# TYPE grpc_server_msg_sent_total counter
grpc_server_msg_sent_total{grpc_type="BIDI_STREAMING",grpc_service="grpc.reflection.v1alpha.ServerReflection",grpc_method="ServerReflectionInfo",} 6.0
`)

type mockedGRPCGetter struct{}

// Get retrieves content from the metrics gRPC endpoint.
func (m *mockedGRPCGetter) Get() ([]byte, string, error) {
	contentType := "text/plain; version=0.0.4"
	return body, contentType, nil
}

func init() {
	l.SetLevel(l.DebugLevel)
}

func getTestNerveGRPC() *nerveGRPCCollector {
	return newNerveGRPC(make(chan metric.Metric), 12, l.WithField("testing", "nervegrpc")).(*nerveGRPCCollector)
}

func TestDefaultConfigNerveGRPC(t *testing.T) {
	inst := getTestNerveGRPC()
	inst.Configure(make(map[string]interface{}))

	assert.Equal(t, 12, inst.Interval())
	assert.Equal(t, 2, inst.timeout)
	assert.Equal(t, "/etc/nerve/nerve.conf.json", inst.configFilePath)
}

func TestConfigNerveGRPC(t *testing.T) {
	cfg := map[string]interface{}{
		"interval":          345,
		"servicesWhitelist": []string{"test_service"},
		"configFilePath":    "/etc/your/moms/house",
		"http_timeout":      12,
	}

	inst := getTestNerveGRPC()
	inst.Configure(cfg)

	assert.Equal(t, 345, inst.Interval())
	assert.Equal(t, "/etc/your/moms/house", inst.configFilePath)
	assert.Equal(t, []string{"test_service"}, inst.servicesWhitelist)
	assert.Equal(t, 12, inst.timeout)
}

func TestQueryService(t *testing.T) {
	inst := getTestNerveGRPC()
	go inst.queryService("grpc_service", 8888, &mockedGRPCGetter{})

	actual := []metric.Metric{}
	lenLines := 7
	for i := 0; i < lenLines; i++ {
		actual = append(actual, <-inst.Channel())
	}

	validateGRPCResults(t, actual, lenLines)
}

func validateGRPCResults(t *testing.T, actual []metric.Metric, length int) {
	assert.Equal(t, length, len(actual))

	for _, m := range actual {
		switch m.Name {
		case "grpc_server_handled_latency_seconds":
			metricTypeDim, exists := m.GetDimensionValue("grpc_type")
			assert.True(t, exists)
			assert.Equal(t, "BIDI_STREAMING", metricTypeDim)
			assert.Equal(t, 0.15, m.Value)
		case "grpc_server_handled_latency_seconds_bucket":
			metricTypeDim, exists := m.GetDimensionValue("grpc_type")
			assert.True(t, exists)
			assert.Equal(t, "BIDI_STREAMING", metricTypeDim)
			assert.Equal(t, 1.0, m.Value)
		case "grpc_server_handled_latency_seconds_count":
			metricTypeDim, exists := m.GetDimensionValue("grpc_type")
			assert.True(t, exists)
			assert.Equal(t, "BIDI_STREAMING", metricTypeDim)
			assert.Equal(t, 1.0, m.Value)
		case "grpc_server_handled_latency_seconds_sum":
			metricTypeDim, exists := m.GetDimensionValue("grpc_type")
			assert.True(t, exists)
			assert.Equal(t, "BIDI_STREAMING", metricTypeDim)
			assert.Equal(t, 0.1, m.Value)
		case "grpc_server_handled_total":
			metricTypeDim, exists := m.GetDimensionValue("grpc_type")
			assert.True(t, exists)
			assert.Equal(t, "BIDI_STREAMING", metricTypeDim)
			assert.Equal(t, 3.0, m.Value)
		case "grpc_server_started_total":
			metricTypeDim, exists := m.GetDimensionValue("grpc_type")
			assert.True(t, exists)
			assert.Equal(t, "BIDI_STREAMING", metricTypeDim)
			assert.Equal(t, 1.0, m.Value)
		case "grpc_server_msg_received_total":
			metricTypeDim, exists := m.GetDimensionValue("grpc_type")
			assert.True(t, exists)
			assert.Equal(t, "BIDI_STREAMING", metricTypeDim)
			assert.Equal(t, 5.0, m.Value)
		case "grpc_server_msg_sent_total":
			metricTypeDim, exists := m.GetDimensionValue("grpc_type")
			assert.True(t, exists)
			assert.Equal(t, "BIDI_STREAMING", metricTypeDim)
			assert.Equal(t, 6.0, m.Value)
		default:
			t.Fatal("Unexpected metric name: " + m.Name)
		}

	}
}
