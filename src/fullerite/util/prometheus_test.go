package util

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fullerite/metric"
)

var body = []byte(`
# HELP go_memstats_last_gc_time_seconds Number of seconds since 1970 of last garbage collection.
# TYPE go_memstats_last_gc_time_seconds gauge
go_memstats_last_gc_time_seconds 1.579737845727797e+09
# HELP process_open_fds Number of open file descriptors.
# TYPE process_open_fds gauge
process_open_fds 30
# HELP process_virtual_memory_bytes Virtual memory size in bytes.
# TYPE process_virtual_memory_bytes counter
process_virtual_memory_bytes 9.850318848e+09
# HELP etcd_server_go_version Which Go version server is running with. 1 for 'server_go_version' label with current version.
# TYPE etcd_server_go_version gauge
etcd_server_go_version{server_go_version="go1.10.8"} 1
# HELP kubelet_docker_operations_latency_microseconds [ALPHA] (Deprecated) Latency in microseconds of Docker operations. Broken down by operation type.
# TYPE kubelet_docker_operations_latency_microseconds summary
kubelet_docker_operations_latency_microseconds{operation_type="stop_container",quantile="0.5"} 123
kubelet_docker_operations_latency_microseconds{operation_type="stop_container",quantile="0.9"} 456
kubelet_docker_operations_latency_microseconds{operation_type="stop_container",quantile="0.99"} 789
kubelet_docker_operations_latency_microseconds_sum{operation_type="stop_container"} 1.165381e+06
kubelet_docker_operations_latency_microseconds_count{operation_type="stop_container"} 202
kubelet_docker_operations_latency_microseconds{operation_type="version",quantile="0.5"} 489
kubelet_docker_operations_latency_microseconds{operation_type="version",quantile="0.9"} 650
kubelet_docker_operations_latency_microseconds{operation_type="version",quantile="0.99"} 1581
kubelet_docker_operations_latency_microseconds_sum{operation_type="version"} 4.0745267e+07
kubelet_docker_operations_latency_microseconds_count{operation_type="version"} 79973
# HELP kubelet_cgroup_manager_duration_seconds [ALPHA] Duration in seconds for cgroup manager operations. Broken down by method.
# TYPE kubelet_cgroup_manager_duration_seconds histogram
kubelet_cgroup_manager_duration_seconds_bucket{operation_type="create",le="0.005"} 2
kubelet_cgroup_manager_duration_seconds_bucket{operation_type="create",le="0.01"} 3
kubelet_cgroup_manager_duration_seconds_bucket{operation_type="create",le="10"} 3
kubelet_cgroup_manager_duration_seconds_bucket{operation_type="create",le="+Inf"} 3
kubelet_cgroup_manager_duration_seconds_sum{operation_type="create"} 0.01842405
kubelet_cgroup_manager_duration_seconds_count{operation_type="create"} 3
kubelet_cgroup_manager_duration_seconds_bucket{operation_type="destroy",le="0.005"} 3
kubelet_cgroup_manager_duration_seconds_bucket{operation_type="destroy",le="0.01"} 3
kubelet_cgroup_manager_duration_seconds_bucket{operation_type="destroy",le="10"} 3
kubelet_cgroup_manager_duration_seconds_bucket{operation_type="destroy",le="+Inf"} 3
kubelet_cgroup_manager_duration_seconds_sum{operation_type="destroy"} 0.001487533
kubelet_cgroup_manager_duration_seconds_count{operation_type="destroy"} 3
`)

const (
	contentType = "text/plain; version=0.0.4"
)

func TestTrimSuffix(t *testing.T) {
	var res string
	var trimmed bool

	res, trimmed = trimSuffix("abcdef", "def")
	assert.Equal(t, "abc", res)
	assert.Equal(t, trimmed, true)

	res, trimmed = trimSuffix("abcdef", "de")
	assert.Equal(t, "abcdef", res)
	assert.Equal(t, trimmed, false)

	res, trimmed = trimSuffix("def", "def")
	assert.Equal(t, "def", res)
	assert.Equal(t, trimmed, false)

	res, trimmed = trimSuffix("cdef", "def")
	assert.Equal(t, "c", res)
	assert.Equal(t, trimmed, true)

	res, trimmed = trimSuffix("ef", "def")
	assert.Equal(t, "ef", res)
	assert.Equal(t, trimmed, false)
}

func TestExtractPrometheusMetrics(t *testing.T) {
	expectedMetrics := []metric.Metric{
		metric.Metric{
			Name:       "go_memstats_last_gc_time_seconds",
			MetricType: metric.Gauge,
			Value:      1.579737845727797e+09,
			Dimensions: map[string]string{},
		},
		metric.Metric{
			Name:       "process_open_fds",
			MetricType: metric.Gauge,
			Value:      30,
			Dimensions: map[string]string{},
		},
		metric.Metric{
			Name:       "process_virtual_memory_bytes",
			MetricType: metric.CumulativeCounter,
			Value:      9.850318848e+09,
			Dimensions: map[string]string{},
		},
		metric.Metric{
			Name:       "etcd_server_go_version",
			MetricType: metric.Gauge,
			Value:      1,
			Dimensions: map[string]string{"server_go_version": "go1.10.8"},
		},
		metric.Metric{
			Name:       "kubelet_docker_operations_latency_microseconds_quantile",
			MetricType: metric.Gauge,
			Value:      123,
			Dimensions: map[string]string{
				"quantile":       "0.5",
				"operation_type": "stop_container",
			},
		},
		metric.Metric{
			Name:       "kubelet_docker_operations_latency_microseconds_quantile",
			MetricType: metric.Gauge,
			Value:      456,
			Dimensions: map[string]string{
				"quantile":       "0.9",
				"operation_type": "stop_container",
			},
		},
		metric.Metric{
			Name:       "kubelet_docker_operations_latency_microseconds_quantile",
			MetricType: metric.Gauge,
			Value:      789,
			Dimensions: map[string]string{
				"quantile":       "0.99",
				"operation_type": "stop_container",
			},
		},
		metric.Metric{
			Name:       "kubelet_docker_operations_latency_microseconds",
			MetricType: metric.CumulativeCounter,
			Value:      1.165381e+06,
			Dimensions: map[string]string{
				"operation_type": "stop_container",
			},
		},
		metric.Metric{
			Name:       "kubelet_docker_operations_latency_microseconds_count",
			MetricType: metric.CumulativeCounter,
			Value:      202,
			Dimensions: map[string]string{
				"operation_type": "stop_container",
			},
		},
		metric.Metric{
			Name:       "kubelet_docker_operations_latency_microseconds_quantile",
			MetricType: metric.Gauge,
			Value:      489,
			Dimensions: map[string]string{
				"quantile":       "0.5",
				"operation_type": "version",
			},
		},
		metric.Metric{
			Name:       "kubelet_docker_operations_latency_microseconds_quantile",
			MetricType: metric.Gauge,
			Value:      650,
			Dimensions: map[string]string{
				"quantile":       "0.9",
				"operation_type": "version",
			},
		},
		metric.Metric{
			Name:       "kubelet_docker_operations_latency_microseconds_quantile",
			MetricType: metric.Gauge,
			Value:      1581,
			Dimensions: map[string]string{
				"quantile":       "0.99",
				"operation_type": "version",
			},
		},
		metric.Metric{
			Name:       "kubelet_docker_operations_latency_microseconds",
			MetricType: metric.CumulativeCounter,
			Value:      4.0745267e+07,
			Dimensions: map[string]string{
				"operation_type": "version",
			},
		},
		metric.Metric{
			Name:       "kubelet_docker_operations_latency_microseconds_count",
			MetricType: metric.CumulativeCounter,
			Value:      79973,
			Dimensions: map[string]string{
				"operation_type": "version",
			},
		},
		metric.Metric{
			Name:       "kubelet_cgroup_manager_duration_seconds_bucket",
			MetricType: metric.CumulativeCounter,
			Value:      2,
			Dimensions: map[string]string{
				"le":             "0.005",
				"operation_type": "create",
			},
		},
		metric.Metric{
			Name:       "kubelet_cgroup_manager_duration_seconds_bucket",
			MetricType: metric.CumulativeCounter,
			Value:      3,
			Dimensions: map[string]string{
				"le":             "0.01",
				"operation_type": "create",
			},
		},
		metric.Metric{
			Name:       "kubelet_cgroup_manager_duration_seconds_bucket",
			MetricType: metric.CumulativeCounter,
			Value:      3,
			Dimensions: map[string]string{
				"le":             "10",
				"operation_type": "create",
			},
		},
		metric.Metric{
			Name:       "kubelet_cgroup_manager_duration_seconds_bucket",
			MetricType: metric.CumulativeCounter,
			Value:      3,
			Dimensions: map[string]string{
				"le":             "+Inf",
				"operation_type": "create",
			},
		},
		metric.Metric{
			Name:       "kubelet_cgroup_manager_duration_seconds",
			MetricType: metric.CumulativeCounter,
			Value:      0.01842405,
			Dimensions: map[string]string{
				"operation_type": "create",
			},
		},
		metric.Metric{
			Name:       "kubelet_cgroup_manager_duration_seconds_count",
			MetricType: metric.CumulativeCounter,
			Value:      3,
			Dimensions: map[string]string{
				"operation_type": "create",
			},
		},
		metric.Metric{
			Name:       "kubelet_cgroup_manager_duration_seconds_bucket",
			MetricType: metric.CumulativeCounter,
			Value:      3,
			Dimensions: map[string]string{
				"le":             "0.005",
				"operation_type": "destroy",
			},
		},
		metric.Metric{
			Name:       "kubelet_cgroup_manager_duration_seconds_bucket",
			MetricType: metric.CumulativeCounter,
			Value:      3,
			Dimensions: map[string]string{
				"le":             "0.01",
				"operation_type": "destroy",
			},
		},
		metric.Metric{
			Name:       "kubelet_cgroup_manager_duration_seconds_bucket",
			MetricType: metric.CumulativeCounter,
			Value:      3,
			Dimensions: map[string]string{
				"le":             "10",
				"operation_type": "destroy",
			},
		},
		metric.Metric{
			Name:       "kubelet_cgroup_manager_duration_seconds_bucket",
			MetricType: metric.CumulativeCounter,
			Value:      3,
			Dimensions: map[string]string{
				"le":             "+Inf",
				"operation_type": "destroy",
			},
		},
		metric.Metric{
			Name:       "kubelet_cgroup_manager_duration_seconds",
			MetricType: metric.CumulativeCounter,
			Value:      0.001487533,
			Dimensions: map[string]string{
				"operation_type": "destroy",
			},
		},
		metric.Metric{
			Name:       "kubelet_cgroup_manager_duration_seconds_count",
			MetricType: metric.CumulativeCounter,
			Value:      3,
			Dimensions: map[string]string{
				"operation_type": "destroy",
			},
		},
	}

	actualMetrics, err := ExtractPrometheusMetrics(body, contentType, nil, nil, "", map[string]string{}, nil)

	assert.Nil(t, err)
	assert.Equal(t, expectedMetrics, actualMetrics)
}

func TestExtractPrometheusMetricsWithPrefixAndWhitelist(t *testing.T) {
	expectedMetrics := []metric.Metric{
		metric.Metric{
			Name:       "123/go_memstats_last_gc_time_seconds",
			MetricType: metric.Gauge,
			Value:      1.579737845727797e+09,
			Dimensions: map[string]string{
				"foo": "bar",
			},
		},
		metric.Metric{
			Name:       "123/process_virtual_memory_bytes",
			MetricType: metric.CumulativeCounter, Value: 9.850318848e+09,
			Dimensions: map[string]string{
				"foo": "bar",
			},
		},
	}

	actualMetrics, err := ExtractPrometheusMetrics(
		body,
		contentType,
		map[string]bool{
			"process_virtual_memory_bytes":     true,
			"go_memstats_last_gc_time_seconds": true,
		},
		nil,
		"123/",
		map[string]string{
			"foo": "bar",
		},
		nil,
	)

	assert.Nil(t, err)
	assert.Equal(t, expectedMetrics, actualMetrics)
}

func TestPrometheusExtractMetricsWithBlacklist(t *testing.T) {
	expectedMetrics := []metric.Metric{
		metric.Metric{
			Name:       "go_memstats_last_gc_time_seconds",
			MetricType: metric.Gauge,
			Value:      1.579737845727797e+09,
			Dimensions: map[string]string{},
		},
		metric.Metric{
			Name:       "process_open_fds",
			MetricType: metric.Gauge,
			Value:      30,
			Dimensions: map[string]string{},
		},
		metric.Metric{
			Name:       "etcd_server_go_version",
			MetricType: metric.Gauge,
			Value:      1,
			Dimensions: map[string]string{"server_go_version": "go1.10.8"},
		},
	}

	actualMetrics, err := ExtractPrometheusMetrics(
		body,
		contentType,
		nil,
		map[string]bool{
			"process_virtual_memory_bytes":                   true,
			"kubelet_docker_operations_latency_microseconds": true,
			"kubelet_cgroup_manager_duration_seconds":        true,
		},
		"",
		map[string]string{},
		nil,
	)

	assert.Nil(t, err)
	assert.Equal(t, expectedMetrics, actualMetrics)
}
