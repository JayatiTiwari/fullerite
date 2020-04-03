package collector

import (
	"encoding/json"
	"fullerite/metric"

	"reflect"
	"testing"

	l "github.com/Sirupsen/logrus"
	"github.com/fsouza/go-dockerclient"
	"github.com/stretchr/testify/assert"
)

func MockObtainRealPath(mountPath string) (string, error) {
	return mountPath, nil
}

func getSUT() *DockerStats {
	expectedChan := make(chan metric.Metric)
	var expectedLogger = defaultLog.WithFields(l.Fields{"collector": "fullerite"})

	return newDockerStats(expectedChan, 10, expectedLogger).(*DockerStats)
}

func TestDockerStatsNewDockerStats(t *testing.T) {
	expectedChan := make(chan metric.Metric)
	var expectedLogger = defaultLog.WithFields(l.Fields{"collector": "fullerite"})
	expectedType := make(map[string]*CPUValues)

	d := newDockerStats(expectedChan, 10, expectedLogger).(*DockerStats)

	assert.Equal(t, d.log, expectedLogger)
	assert.Equal(t, d.channel, expectedChan)
	assert.Equal(t, d.interval, 10)
	assert.Equal(t, d.name, "DockerStats")
	assert.Equal(t, reflect.TypeOf(d.previousCPUValues), reflect.TypeOf(expectedType))
	assert.Equal(t, len(d.previousCPUValues), 0)
	d.Configure(make(map[string]interface{}))
	assert.Equal(t, d.GetEndpoint(), endpoint)
}

func TestDockerStatsConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})

	d := newDockerStats(nil, 123, nil).(*DockerStats)
	d.Configure(config)

	assert.Equal(t, 123, d.Interval())
}

func TestDockerStatsConfigure(t *testing.T) {
	config := make(map[string]interface{})
	config["interval"] = 9999

	d := newDockerStats(nil, 123, nil).(*DockerStats)
	d.Configure(config)

	assert.Equal(t, 9999, d.Interval())
}

func TestDockerStatsBuildMetrics(t *testing.T) {
	config := make(map[string]interface{})
	envVars := []byte(`
	{
		"service_name":  {
			"MESOS_TASK_ID": "[^\\.]*"
		},
		"instance_name": {
			"MESOS_TASK_ID": "\\.([^\\.]*)\\."}
	}`)
	var val map[string]interface{}
	// prepare disk stats io stats dummy test data
	diskStats := make(map[string][]string)
	statVals := []string{"202", "1", "nvme0n1", "39757175", "4069", "1474473221", "15781084", "195016023", "83149828", "4027089904", "134530008", "0", "98404384", "150301288"}
	diskStats["nvme0n1"] = statVals

	var diskIOStatsList []DiskIOStats
	deviceOne := DiskIOStats{"nvme0n1", 202, 2, "/nail", 3.9686986, 1.9316838}
	diskIOStatsList = append(diskIOStatsList, deviceOne)

	err := json.Unmarshal(envVars, &val)
	assert.Equal(t, err, nil)
	config["generatedDimensions"] = val

	stats := new(docker.Stats)
	stats.Networks = make(map[string]docker.NetworkStats)
	stats.Networks["eth0"] = docker.NetworkStats{RxBytes: 10, TxBytes: 20}
	stats.MemoryStats.Stats.Rss = 50
	stats.MemoryStats.Limit = 70
	stats.CPUStats.ThrottlingData.ThrottledPeriods = 123
	stats.CPUStats.ThrottlingData.ThrottledTime = 456
	stats.BlkioStats.IOServiceBytesRecursive = []docker.BlkioStatsEntry{
		docker.BlkioStatsEntry{
			Major: 1,
			Minor: 2,
			Op:    "Read",
			Value: 1234,
		},
		docker.BlkioStatsEntry{
			Major: 3,
			Minor: 4,
			Op:    "Write",
			Value: 5678,
		},
	}
	stats.BlkioStats.IOServicedRecursive = []docker.BlkioStatsEntry{
		docker.BlkioStatsEntry{
			Major: 3,
			Minor: 4,
			Op:    "Total",
			Value: 1111,
		},
	}

	containerJSON := []byte(`
	{
		"ID": "test-id",
		"Name": "test-container",
		"SizeRw": 1234,
		"SizeRootFs": 5678,
		"Config": {
			"Env": [
				"MESOS_TASK_ID=my--service.main.blablagit6bdsadnoise"
			]
		}
	}`)
	var container *docker.Container
	err = json.Unmarshal(containerJSON, &container)
	assert.Equal(t, err, nil)

	baseDims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"service_name":   "my_service",
		"instance_name":  "main",
	}
	netDims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"service_name":   "my_service",
		"instance_name":  "main",
		"iface":          "eth0",
	}
	dev12Dims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"service_name":   "my_service",
		"instance_name":  "main",
		"blkdev":         "1:2",
	}
	dev34Dims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"service_name":   "my_service",
		"instance_name":  "main",
		"blkdev":         "3:4",
	}

	expectedDimsGen := map[string]string{
		"service_name":  "my_service",
		"instance_name": "main",
	}
	expectedMetrics := []metric.Metric{
		metric.Metric{"DockerMemoryUsed", "gauge", 50, baseDims},
		metric.Metric{"DockerMemoryLimit", "gauge", 70, baseDims},
		metric.Metric{"DockerCpuPercentage", "gauge", 0.5, baseDims},
		metric.Metric{"DockerCpuThrottledPeriods", "cumcounter", 123, baseDims},
		metric.Metric{"DockerCpuThrottledNanoseconds", "cumcounter", 456, baseDims},
		metric.Metric{"DockerLocalDiskUsed", "gauge", 1234, baseDims},
		metric.Metric{"DockerImageLocalDiskUsed", "gauge", 5678, baseDims},
		metric.Metric{"DockerTxBytes", "cumcounter", 20, netDims},
		metric.Metric{"DockerRxBytes", "cumcounter", 10, netDims},
		metric.Metric{"DockerBlkDeviceReadBytes", "cumcounter", 1234, dev12Dims},
		metric.Metric{"DockerBlkDeviceWriteBytes", "cumcounter", 5678, dev34Dims},
		metric.Metric{"DockerBlkDeviceTotalRequests", "cumcounter", 1111, dev34Dims},
		metric.Metric{"DockerContainerCount", "counter", 1, expectedDimsGen},
	}

	d := getSUT()
	d.Configure(config)
	ret := d.buildMetrics(container, stats, 0.5, diskStats, diskIOStatsList, MockObtainRealPath)
	assert.Equal(t, ret, expectedMetrics)
}

func TestDockerStatsBuildMetricsWithEmitDiskMetrics(t *testing.T) {
	config := make(map[string]interface{})
	envVars := []byte(`
        {
                "service_name":  {
                        "MESOS_TASK_ID": "[^\\.]*"
                },
                "instance_name": {
                        "MESOS_TASK_ID": "\\.([^\\.]*)\\."}
        }`)
	var val map[string]interface{}

	// prepare disk stats io stats dummy test data
	diskStats := make(map[string][]string)
	statVals := []string{"202", "1", "testDevice", "39757175", "4069", "1474473221", "15781084", "195016023", "83149828", "4027089904", "134530008", "0", "98404384", "150301288"}
	diskStats["test-id"] = statVals

	var diskIOStatsList []DiskIOStats
	diskIOStatsList = append(diskIOStatsList, DiskIOStats{"testDevice", 202, 0, "testSource", 1000, 1000})

	err := json.Unmarshal(envVars, &val)
	assert.Equal(t, err, nil)
	config["generatedDimensions"] = val

	stats := new(docker.Stats)
	stats.Networks = make(map[string]docker.NetworkStats)
	stats.Networks["eth0"] = docker.NetworkStats{RxBytes: 10, TxBytes: 20}
	stats.MemoryStats.Stats.Rss = 50
	stats.MemoryStats.Limit = 70
	stats.CPUStats.ThrottlingData.ThrottledPeriods = 123
	stats.CPUStats.ThrottlingData.ThrottledTime = 456
	stats.BlkioStats.IOServiceBytesRecursive = []docker.BlkioStatsEntry{
		docker.BlkioStatsEntry{
			Major: 1,
			Minor: 2,
			Op:    "Read",
			Value: 1234,
		},
		docker.BlkioStatsEntry{
			Major: 3,
			Minor: 4,
			Op:    "Write",
			Value: 5678,
		},
	}
	stats.BlkioStats.IOServicedRecursive = []docker.BlkioStatsEntry{
		docker.BlkioStatsEntry{
			Major: 3,
			Minor: 4,
			Op:    "Total",
			Value: 1111,
		},
	}

	containerJSON := []byte(`
        {
                "ID": "test-id",
                "Name": "test-container",
                "SizeRw": 1234,
                "SizeRootFs": 5678,
                "Config": {
                "Labels": {
                        "io.kubernetes.container.name": "test_name"
                },
                        "Env": [
                                "MESOS_TASK_ID=my--service.main.blablagit6bdsadnoise",
                                "PAASTA_INSTANCE=test_instance",
                                "PAASTA_SERVICE=test_service",
                                "PAASTA_CLUSTER=test_cluster"
                        ]
                },
                "Mounts": [
                        {
                                "Type": "bind",
                                "Source": "testSource",
                                "Destination": "test_path",
                                "Propagation": "rprivate",
                                "Mode": "testMode",
                                "RW": true
                        }
                ]
        }`)
	var container *docker.Container
	err = json.Unmarshal(containerJSON, &container)
	assert.Equal(t, err, nil)

	baseDims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"service_name":   "my_service",
		"instance_name":  "main",
	}
	netDims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"service_name":   "my_service",
		"instance_name":  "main",
		"iface":          "eth0",
	}
	dev12Dims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"service_name":   "my_service",
		"instance_name":  "main",
		"blkdev":         "1:2",
	}
	dev34Dims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"service_name":   "my_service",
		"instance_name":  "main",
		"blkdev":         "3:4",
	}
	expectedDimsGen := map[string]string{
		"service_name":  "my_service",
		"instance_name": "main",
	}
	expectedDimsDisk := map[string]string{
		"container_name":       "test_name",
		"container_mount_path": "test_path",
		"paasta_service":       "test_service",
		"paasta_instance":      "test_instance",
		"paasta_cluster":       "test_cluster",
	}

	expectedMetrics := []metric.Metric{
		metric.Metric{"DockerMemoryUsed", "gauge", 50, baseDims},
		metric.Metric{"DockerMemoryLimit", "gauge", 70, baseDims},
		metric.Metric{"DockerCpuPercentage", "gauge", 0.5, baseDims},
		metric.Metric{"DockerCpuThrottledPeriods", "cumcounter", 123, baseDims},
		metric.Metric{"DockerCpuThrottledNanoseconds", "cumcounter", 456, baseDims},
		metric.Metric{"DockerLocalDiskUsed", "gauge", 1234, baseDims},
		metric.Metric{"DockerImageLocalDiskUsed", "gauge", 5678, baseDims},
		metric.Metric{"DockerTxBytes", "cumcounter", 20, netDims},
		metric.Metric{"DockerRxBytes", "cumcounter", 10, netDims},
		metric.Metric{"DockerBlkDeviceReadBytes", "cumcounter", 1234, dev12Dims},
		metric.Metric{"DockerBlkDeviceWriteBytes", "cumcounter", 5678, dev34Dims},
		metric.Metric{"DockerBlkDeviceTotalRequests", "cumcounter", 1111, dev34Dims},
		metric.Metric{"DockerContainerCount", "counter", 1, expectedDimsGen},
		metric.Metric{"DockerDiskReads", "gauge", 1000, expectedDimsDisk},
		metric.Metric{"DockerDiskWrites", "gauge", 1000, expectedDimsDisk},
		metric.Metric{"DockerDiskIO", "gauge", 2000, expectedDimsDisk},
	}

	d := getSUT()
	d.Configure(config)
	d.emitDiskMetrics = true
	var ret []metric.Metric
	d.buildMetrics(container, stats, 0.5, diskStats, diskIOStatsList, MockObtainRealPath)
	// since the first value of any meteric is always 0, adding another record
	diskIOStatsList = nil
	diskIOStatsList = append(diskIOStatsList, DiskIOStats{"testDevice", 202, 0, "testSource", 2000, 2000})
	ret = d.buildMetrics(container, stats, 0.5, diskStats, diskIOStatsList, MockObtainRealPath)
	assert.Equal(t, ret, expectedMetrics)
}

func TestDockerStatsBuildwithEmitImageName(t *testing.T) {
	config := make(map[string]interface{})
	envVars := []byte(`
	{
		"service_name":  {
			"MESOS_TASK_ID": "[^\\.]*"
		},
		"instance_name": {
			"MESOS_TASK_ID": "\\.([^\\.]*)\\."}
	}`)
	var val map[string]interface{}

	// prepare disk stats io stats dummy test data
	diskStats := make(map[string][]string)
	statVals := []string{"202", "1", "test-id", "39757175", "4069", "1474473221", "15781084", "195016023", "83149828", "4027089904", "134530008", "0", "98404384", "150301288"}
	diskStats["test-id"] = statVals

	var diskIOStatsList []DiskIOStats
	diskIOStatsList = append(diskIOStatsList, DiskIOStats{"test-id", 202, 0, "testSource", 0, 0})

	err := json.Unmarshal(envVars, &val)
	assert.Equal(t, err, nil)
	config["generatedDimensions"] = val

	stats := new(docker.Stats)
	stats.Networks = make(map[string]docker.NetworkStats)
	stats.Networks["eth0"] = docker.NetworkStats{RxBytes: 10, TxBytes: 20}
	stats.MemoryStats.Stats.Rss = 50
	stats.MemoryStats.Stats.Swap = 10
	stats.MemoryStats.Limit = 70
	stats.CPUStats.ThrottlingData.ThrottledPeriods = 123
	stats.CPUStats.ThrottlingData.ThrottledTime = 456

	containerJSON := []byte(`
	{
        "ID": "test-id",
		"Name": "test-container",
		"Config": {
			"Env": [
				"MESOS_TASK_ID=my--service.main.blablagit6bdsadnoise"
			],
            "Image": "test image"
		}
	}`)
	var container *docker.Container
	err = json.Unmarshal(containerJSON, &container)
	assert.Equal(t, err, nil)

	baseDims := map[string]string{
		"image_name":    "test image",
		"service_name":  "my_service",
		"instance_name": "main",
	}
	netDims := map[string]string{
		"image_name":    "test image",
		"iface":         "eth0",
		"service_name":  "my_service",
		"instance_name": "main",
	}

	expectedDimsGen := map[string]string{
		"service_name":  "my_service",
		"instance_name": "main",
	}
	expectedMetrics := []metric.Metric{
		metric.Metric{"DockerMemoryUsed", "gauge", 60, baseDims},
		metric.Metric{"DockerMemoryLimit", "gauge", 70, baseDims},
		metric.Metric{"DockerCpuPercentage", "gauge", 0.5, baseDims},
		metric.Metric{"DockerCpuThrottledPeriods", "cumcounter", 123, baseDims},
		metric.Metric{"DockerCpuThrottledNanoseconds", "cumcounter", 456, baseDims},
		metric.Metric{"DockerLocalDiskUsed", "gauge", 0, baseDims},
		metric.Metric{"DockerImageLocalDiskUsed", "gauge", 0, baseDims},
		metric.Metric{"DockerTxBytes", "cumcounter", 20, netDims},
		metric.Metric{"DockerRxBytes", "cumcounter", 10, netDims},
		metric.Metric{"DockerContainerCount", "counter", 1, expectedDimsGen},
	}

	d := getSUT()
	d.Configure(config)
	d.emitImageName = true
	ret := d.buildMetrics(container, stats, 0.5, diskStats, diskIOStatsList, MockObtainRealPath)
	assert.Equal(t, ret, expectedMetrics)
}

func TestDockerStatsBuildMetricsWithNameAsEnvVariable(t *testing.T) {
	config := make(map[string]interface{})
	envVars := []byte(`
	{
		"service_name": {
			"SERVICE_NAME": ".*"
		}
	}`)
	var val map[string]interface{}
	// prepare disk stats io stats dummy test data
	diskStats := make(map[string][]string)
	statVals := []string{"202", "1", "test-id", "39757175", "4069", "1474473221", "15781084", "195016023", "83149828", "4027089904", "134530008", "0", "98404384", "150301288"}
	diskStats["test-id"] = statVals

	var diskIOStatsList []DiskIOStats
	diskIOStatsList = append(diskIOStatsList, DiskIOStats{"test-id", 202, 0, "testSource", 0, 0})

	err := json.Unmarshal(envVars, &val)
	assert.Equal(t, err, nil)
	config["generatedDimensions"] = val

	stats := new(docker.Stats)
	stats.MemoryStats.Stats.Rss = 50
	stats.MemoryStats.Limit = 70
	stats.CPUStats.ThrottlingData.ThrottledPeriods = 123
	stats.CPUStats.ThrottlingData.ThrottledTime = 456

	containerJSON := []byte(`
	{
		"ID": "test-id",
		"Name": "test-container",
		"Config": {
			"Env": [
				"SERVICE_NAME=my_service"
			]
		}
	}`)
	var container *docker.Container
	err = json.Unmarshal(containerJSON, &container)
	assert.Equal(t, err, nil)

	expectedDims := map[string]string{
		"container_id":   "test-id",
		"container_name": "test-container",
		"service_name":   "my_service",
	}
	expectedDimsGen := map[string]string{
		"service_name": "my_service",
	}

	expectedMetrics := []metric.Metric{
		metric.Metric{"DockerMemoryUsed", "gauge", 50, expectedDims},
		metric.Metric{"DockerMemoryLimit", "gauge", 70, expectedDims},
		metric.Metric{"DockerCpuPercentage", "gauge", 0.5, expectedDims},
		metric.Metric{"DockerCpuThrottledPeriods", "cumcounter", 123, expectedDims},
		metric.Metric{"DockerCpuThrottledNanoseconds", "cumcounter", 456, expectedDims},
		metric.Metric{"DockerLocalDiskUsed", "gauge", 0, expectedDims},
		metric.Metric{"DockerImageLocalDiskUsed", "gauge", 0, expectedDims},
		metric.Metric{"DockerContainerCount", "counter", 1, expectedDimsGen},
	}

	d := getSUT()
	d.Configure(config)
	ret := d.buildMetrics(container, stats, 0.5, diskStats, diskIOStatsList, MockObtainRealPath)

	assert.Equal(t, ret, expectedMetrics)
}

func TestDockerStatsCalculateCPUPercent(t *testing.T) {
	var previousTotalUsage = uint64(0)
	var previousSystem = uint64(0)

	stats := new(docker.Stats)
	stats.CPUStats.CPUUsage.PercpuUsage = make([]uint64, 24)
	stats.CPUStats.CPUUsage.TotalUsage = 1261158030354
	stats.CPUStats.SystemCPUUsage = 108086414700000000

	assert.Equal(t, 0.02800332753427522, calculateCPUPercent(previousTotalUsage, previousSystem, stats))

	previousTotalUsage = stats.CPUStats.CPUUsage.TotalUsage
	previousSystem = stats.CPUStats.SystemCPUUsage
	stats.CPUStats.CPUUsage.TotalUsage = 1261164064229
	stats.CPUStats.SystemCPUUsage = 108086652820000000

	assert.Equal(t, 0.060815135225936505, calculateCPUPercent(previousTotalUsage, previousSystem, stats))
}
