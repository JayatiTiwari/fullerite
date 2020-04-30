package collector

import (
	"bufio"
	"fmt"
	"fullerite/config"
	"fullerite/metric"
	"io"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	l "github.com/Sirupsen/logrus"

	"github.com/fsouza/go-dockerclient"

	"github.com/yookoala/realpath"
)

const (
	endpoint    = "unix:///var/run/docker.sock"
	byte_unit   = "byte"
	sector_size = 512
)

type RealPathGetter func(mountPath string) (string, error)

// DockerStats collector type.
// previousCPUValues contains the last cpu-usage values per container.
// dockerClient is the client for the Docker remote API.
type DockerStats struct {
	baseCollector
	previousCPUValues  map[string]*CPUValues
	dockerClient       *docker.Client
	statsTimeout       int
	compiledRegex      map[string]*Regex
	skipRegex          *regexp.Regexp
	endpoint           string
	mu                 *sync.Mutex
	emitImageName      bool
	emitDiskMetrics    bool
	lastValues         map[string]float64
	lastCollectionTime time.Time
	maxValues          map[string]float64
}

// CPUValues struct contains the last cpu-usage values in order to compute properly the current values.
// (see calculateCPUPercent() for more details)
type CPUValues struct {
	totCPU, systemCPU uint64
}

// Regex struct contains the info used to get the user specific dimensions from the docker env variables
// tag: is the environmental variable you want to get the value from
// regex: is the reg exp used to extract the value from the env var
type Regex struct {
	tag   string
	regex *regexp.Regexp
}

// DiskIOStats contains disk stats needed for deriving the IO metric of the device.
type DiskIOStats struct {
	deviceName             string
	minor                  int
	major                  int
	mountPath              string
	reads                  float64
	readsMerged            float64
	readsSectors           float64
	readsMilliseconds      float64
	writes                 float64
	writesMerged           float64
	writesSectors          float64
	writesMilliseconds     float64
	ioInProgress           float64
	ioMilliseconds         float64
	ioMillisecondsWeighted float64
}

// DiskPaastaStats contains the PaaSTA information of the container using this device as a mount.
type DiskIOPaastaStats struct {
	containerName          string
	deviceName             string
	paastaService          string
	paastaInstance         string
	paastaCluster          string
	reads                  float64
	readsMerged            float64
	readsByte              float64
	readsMilliseconds      float64
	writes                 float64
	writesMerged           float64
	writesByte             float64
	writesMilliseconds     float64
	ioInProgress           float64
	ioMilliseconds         float64
	ioMillisecondsWeighted float64
	containerMountPath     string
}

func init() {
	RegisterCollector("DockerStats", newDockerStats)
}

// newDockerStats creates a new DockerStats collector.
func newDockerStats(channel chan metric.Metric, initialInterval int, log *l.Entry) Collector {
	d := new(DockerStats)

	d.log = log
	d.channel = channel
	d.interval = initialInterval
	d.mu = new(sync.Mutex)

	d.name = "DockerStats"
	d.previousCPUValues = make(map[string]*CPUValues)
	d.compiledRegex = make(map[string]*Regex)
	d.emitImageName = false
	d.emitDiskMetrics = false
	d.lastValues = make(map[string]float64)
	d.maxValues = make(map[string]float64)
	// value (2 ** 32) - 1
	d.maxValues["reads"] = 4294967295
	d.maxValues["readsMerged"] = 4294967295
	d.maxValues["readsMilliseconds"] = 4294967295
	d.maxValues["writes"] = 4294967295
	d.maxValues["writesMerged"] = 4294967295
	d.maxValues["writesMilliseconds"] = 4294967295
	d.maxValues["ioMilliseconds"] = 4294967295
	d.maxValues["ioMillisecondsWeighted"] = 4294967295
	// value (2 ** 64) - 1
	d.maxValues["readsSectors"] = 1.8446744e+19
	d.maxValues["writesSectors"] = 1.8446744e+19
	return d
}

// GetEndpoint Returns endpoint of DockerStats instance
func (d *DockerStats) GetEndpoint() string {
	return d.endpoint
}

// Configure takes a dictionary of values with which the handler can configure itself.
func (d *DockerStats) Configure(configMap map[string]interface{}) {
	if timeout, exists := configMap["dockerStatsTimeout"]; exists {
		d.statsTimeout = min(config.GetAsInt(timeout, d.interval), d.interval)
	} else {
		d.statsTimeout = d.interval
	}
	if dockerEndpoint, exists := configMap["dockerEndPoint"]; exists {
		if str, ok := dockerEndpoint.(string); ok {
			d.endpoint = str
		} else {
			d.log.Warn("Failed to cast dokerEndPoint: ", reflect.TypeOf(dockerEndpoint))
		}
	} else {
		d.endpoint = endpoint
	}
	if emitImageName, exists := configMap["emit_image_name"]; exists {
		if boolean, ok := emitImageName.(bool); ok {
			d.emitImageName = boolean
		} else {
			d.log.Warn("Failed to cast emit_image_name: ", reflect.TypeOf(emitImageName))
		}
	}
	if emitDiskMetrics, exists := configMap["emit_disk_metrics"]; exists {
		if boolean, ok := emitDiskMetrics.(bool); ok {
			d.emitDiskMetrics = boolean
		} else {
			d.log.Warn("Failed to cast emitDiskMetrics: ", reflect.TypeOf(emitDiskMetrics))
		}
	}

	d.dockerClient, _ = docker.NewClient(d.endpoint)
	if generatedDimensions, exists := configMap["generatedDimensions"]; exists {
		for dimension, generator := range generatedDimensions.(map[string]interface{}) {
			for key, regx := range config.GetAsMap(generator) {
				re, err := regexp.Compile(regx)
				if err != nil {
					d.log.Warn("Failed to compile regex: ", regx, err)
				} else {
					d.compiledRegex[dimension] = &Regex{regex: re, tag: key}
				}
			}
		}
	}
	d.configureCommonParams(configMap)
	if skipRegex, skipExists := configMap["skipContainerRegex"]; skipExists {
		d.skipRegex = regexp.MustCompile(skipRegex.(string))
	}
}

// Collect iterates on all the docker containers alive and, if possible, collects the correspondent
// memory and cpu statistics.
// For each container a gorutine is started to spin up the collection process.
func (d *DockerStats) Collect() {
	diskStats := make(map[string][]string)
	var diskIOStatsList []DiskIOStats

	if d.dockerClient == nil {
		d.log.Error("Invalid endpoint: ", docker.ErrInvalidEndpoint)
		return
	}
	containers, err := d.dockerClient.ListContainers(docker.ListContainersOptions{All: false})
	if err != nil {
		d.log.Error("ListContainers() failed: ", err)
		return
	}

	if d.emitDiskMetrics {
		// Obtain the disk stats for this device. This is common for all the containers, hence, calculating before iterating over all the containers.
		diskStats, err = d.ObtainDiskStats()
		if err != nil && err != io.EOF {
			d.log.Error("ObtainDiskStats() failed: ", err)
		}
		// join the disk stats with the IO stats
		diskIOStatsList, err = d.ObtainDiskIOStats(diskStats)
		if err != nil && err != io.EOF {
			d.log.Error("ObtainDiskIOStats() failed: ", err)
		}
	}

	for _, apiContainer := range containers {
		container, err := d.dockerClient.InspectContainerWithOptions(docker.InspectContainerOptions{
			ID:   apiContainer.ID,
			Size: true,
		})

		if err != nil {
			d.log.Error("InspectContainerWithOptions() failed: ", err)
			continue
		}

		if d.skipRegex != nil && d.skipRegex.MatchString(container.Name) {
			d.log.Info("Skip container: ", container.Name)
			continue
		}

		if _, ok := d.previousCPUValues[container.ID]; !ok {
			d.previousCPUValues[container.ID] = new(CPUValues)
		}
		go d.getDockerContainerInfo(container, diskStats, diskIOStatsList)
	}
}

// getDockerContainerInfo gets container statistics for the given container.
// results is a channel to make possible the synchronization between the main process and the gorutines (wait-notify pattern).
func (d *DockerStats) getDockerContainerInfo(container *docker.Container, diskStats map[string][]string, diskIOStatsList []DiskIOStats) {
	errC := make(chan error, 1)
	statsC := make(chan *docker.Stats, 1)
	done := make(chan bool, 1)

	go func() {
		errC <- d.dockerClient.Stats(docker.StatsOptions{
			ID:      container.ID,
			Stats:   statsC,
			Stream:  false,
			Done:    done,
			Timeout: time.Second * time.Duration(d.interval)})
	}()
	select {
	case stats, ok := <-statsC:
		if !ok {
			err := <-errC
			d.log.Error("Failed to collect docker container stats: ", err)
			break
		}
		done <- true

		metrics := d.extractMetrics(container, stats, diskStats, diskIOStatsList)
		d.sendMetrics(metrics)

		break
	case <-time.After(time.Duration(d.statsTimeout) * time.Second):
		d.log.Error("Timed out collecting stats for container ", container.ID)
		done <- true
		break
	}
}

func (d *DockerStats) extractMetrics(container *docker.Container, stats *docker.Stats, diskStats map[string][]string, diskIOStatsList []DiskIOStats) []metric.Metric {
	d.mu.Lock()
	defer d.mu.Unlock()
	metrics := d.buildMetrics(container, stats, calculateCPUPercent(d.previousCPUValues[container.ID].totCPU, d.previousCPUValues[container.ID].systemCPU, stats), diskStats, diskIOStatsList, ObtainRealPath)

	d.previousCPUValues[container.ID].totCPU = stats.CPUStats.CPUUsage.TotalUsage
	d.previousCPUValues[container.ID].systemCPU = stats.CPUStats.SystemCPUUsage
	return metrics
}

// buildMetrics creates the actual metrics for the given container.
func (d DockerStats) buildMetrics(container *docker.Container, containerStats *docker.Stats, cpuPercentage float64, diskStats map[string][]string, diskIOStatsList []DiskIOStats, realPathGetterFunc RealPathGetter) []metric.Metric {
	// Report only Rss, not cache.
	mem := containerStats.MemoryStats.Stats.Rss + containerStats.MemoryStats.Stats.Swap
	ret := []metric.Metric{
		buildDockerMetric("DockerMemoryUsed", metric.Gauge, float64(mem)),
		buildDockerMetric("DockerMemoryLimit", metric.Gauge, float64(containerStats.MemoryStats.Limit)),
		buildDockerMetric("DockerCpuPercentage", metric.Gauge, cpuPercentage),
		buildDockerMetric("DockerCpuThrottledPeriods", metric.CumulativeCounter, float64(containerStats.CPUStats.ThrottlingData.ThrottledPeriods)),
		buildDockerMetric("DockerCpuThrottledNanoseconds", metric.CumulativeCounter, float64(containerStats.CPUStats.ThrottlingData.ThrottledTime)),
		buildDockerMetric("DockerLocalDiskUsed", metric.Gauge, float64(container.SizeRw)),
		buildDockerMetric("DockerImageLocalDiskUsed", metric.Gauge, float64(container.SizeRootFs)),
	}
	for netiface := range containerStats.Networks {
		// legacy format
		txb := buildDockerMetric("DockerTxBytes", metric.CumulativeCounter, float64(containerStats.Networks[netiface].TxBytes))
		txb.AddDimension("iface", netiface)
		ret = append(ret, txb)
		rxb := buildDockerMetric("DockerRxBytes", metric.CumulativeCounter, float64(containerStats.Networks[netiface].RxBytes))
		rxb.AddDimension("iface", netiface)
		ret = append(ret, rxb)
	}

	ret = append(ret, metricsForBlkioStatsEntries(containerStats.BlkioStats.IOServiceBytesRecursive, "DockerBlkDevice%sBytes")...)
	ret = append(ret, metricsForBlkioStatsEntries(containerStats.BlkioStats.IOServicedRecursive, "DockerBlkDevice%sRequests")...)

	additionalDimensions := map[string]string{}
	if d.emitImageName {
		stringList := strings.Split(container.Config.Image, ":")
		additionalDimensions = map[string]string{
			"image_name": stringList[0],
		}
	} else {
		additionalDimensions = map[string]string{
			"container_id":   container.ID,
			"container_name": strings.TrimPrefix(container.Name, "/"),
		}
	}
	metric.AddToAll(&ret, additionalDimensions)
	ret = append(ret, buildDockerMetric("DockerContainerCount", metric.Counter, 1))
	metric.AddToAll(&ret, d.extractDimensions(container))

	if d.emitDiskMetrics {
		var timeDelta float64
		// Handle collection time intervals correctly
		collectTime := time.Now()
		timeDelta = float64(d.interval)
		if !d.lastCollectionTime.IsZero() {
			timeDelta = collectTime.Sub(d.lastCollectionTime).Seconds()
		}
		d.lastCollectionTime = collectTime
		// get the IO and PaaSTA stats for this container
		paastaIOStatsList := d.ObtainDiskIOAndPaastaStats(container, diskIOStatsList, realPathGetterFunc)
		for _, record := range paastaIOStatsList {
			io := record.writes + record.reads
			// matching the diskusage collectors logic at https://github.com/Yelp/fullerite/blob/a23d7daea4f894e517959b7e84f61a5b2b9c93c2/src/diamond/collectors/diskusage/diskusage.py#L281
			if io > 0 {
				ioStatRead := buildDockerMetric("DockerDiskReads", metric.Gauge, record.reads)
				ioStatRead.AddDimension("container_name", record.containerName)
				ioStatRead.AddDimension("container_mount_path", record.containerMountPath)
				ioStatRead.AddDimension("paasta_service", record.paastaService)
				ioStatRead.AddDimension("paasta_instance", record.paastaInstance)
				ioStatRead.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatRead)

				ioStatWrite := buildDockerMetric("DockerDiskWrites", metric.Gauge, record.writes)
				ioStatWrite.AddDimension("container_name", record.containerName)
				ioStatWrite.AddDimension("container_mount_path", record.containerMountPath)
				ioStatWrite.AddDimension("paasta_service", record.paastaService)
				ioStatWrite.AddDimension("paasta_instance", record.paastaInstance)
				ioStatWrite.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatWrite)

				ioStat := buildDockerMetric("DockerDiskIO", metric.Gauge, io)
				ioStat.AddDimension("container_name", record.containerName)
				ioStat.AddDimension("container_mount_path", record.containerMountPath)
				ioStat.AddDimension("paasta_service", record.paastaService)
				ioStat.AddDimension("paasta_instance", record.paastaInstance)
				ioStat.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStat)

				ioStatWritesMerged := buildDockerMetric("DockerDiskWritesMerged", metric.Gauge, record.writesMerged)
				ioStatWritesMerged.AddDimension("container_name", record.containerName)
				ioStatWritesMerged.AddDimension("container_mount_path", record.containerMountPath)
				ioStatWritesMerged.AddDimension("paasta_service", record.paastaService)
				ioStatWritesMerged.AddDimension("paasta_instance", record.paastaInstance)
				ioStatWritesMerged.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatWritesMerged)

				ioStatReadsMerged := buildDockerMetric("DockerDiskReadsMerged", metric.Gauge, record.readsMerged)
				ioStatReadsMerged.AddDimension("container_name", record.containerName)
				ioStatReadsMerged.AddDimension("container_mount_path", record.containerMountPath)
				ioStatReadsMerged.AddDimension("paasta_service", record.paastaService)
				ioStatReadsMerged.AddDimension("paasta_instance", record.paastaInstance)
				ioStatReadsMerged.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatReadsMerged)

				ioStatWritesByte := buildDockerMetric("DockerDiskWritesByte", metric.Gauge, record.writesByte)
				ioStatWritesByte.AddDimension("container_name", record.containerName)
				ioStatWritesByte.AddDimension("container_mount_path", record.containerMountPath)
				ioStatWritesByte.AddDimension("paasta_service", record.paastaService)
				ioStatWritesByte.AddDimension("paasta_instance", record.paastaInstance)
				ioStatWritesByte.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatWritesByte)

				ioStatReadsByte := buildDockerMetric("DockerDiskReadsByte", metric.Gauge, record.readsByte)
				ioStatReadsByte.AddDimension("container_name", record.containerName)
				ioStatReadsByte.AddDimension("container_mount_path", record.containerMountPath)
				ioStatReadsByte.AddDimension("paasta_service", record.paastaService)
				ioStatReadsByte.AddDimension("paasta_instance", record.paastaInstance)
				ioStatReadsByte.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatReadsByte)

				readRequestsMergedPerSecond := record.readsMerged / timeDelta
				ioStatReadRequestsMergedPerSecond := buildDockerMetric("DockerDiskReadRequestsMergedPerSecond", metric.Gauge, readRequestsMergedPerSecond)
				ioStatReadRequestsMergedPerSecond.AddDimension("container_name", record.containerName)
				ioStatReadRequestsMergedPerSecond.AddDimension("container_mount_path", record.containerMountPath)
				ioStatReadRequestsMergedPerSecond.AddDimension("paasta_service", record.paastaService)
				ioStatReadRequestsMergedPerSecond.AddDimension("paasta_instance", record.paastaInstance)
				ioStatReadRequestsMergedPerSecond.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatReadRequestsMergedPerSecond)

				writesRequestsMergedPerSecond := record.writesMerged / timeDelta
				ioStatWritesRequestsMergedPerSecond := buildDockerMetric("DockerDiskWritesRequestsMergedPerSecond", metric.Gauge, writesRequestsMergedPerSecond)
				ioStatWritesRequestsMergedPerSecond.AddDimension("container_name", record.containerName)
				ioStatWritesRequestsMergedPerSecond.AddDimension("container_mount_path", record.containerMountPath)
				ioStatWritesRequestsMergedPerSecond.AddDimension("paasta_service", record.paastaService)
				ioStatWritesRequestsMergedPerSecond.AddDimension("paasta_instance", record.paastaInstance)
				ioStatWritesRequestsMergedPerSecond.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatWritesRequestsMergedPerSecond)

				readsPerSecond := record.reads / timeDelta
				ioStatReadsPerSecond := buildDockerMetric("DockerDiskReadsPerSecond", metric.Gauge, readsPerSecond)
				ioStatReadsPerSecond.AddDimension("container_name", record.containerName)
				ioStatReadsPerSecond.AddDimension("container_mount_path", record.containerMountPath)
				ioStatReadsPerSecond.AddDimension("paasta_service", record.paastaService)
				ioStatReadsPerSecond.AddDimension("paasta_instance", record.paastaInstance)
				ioStatReadsPerSecond.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatReadsPerSecond)

				writesPerSecond := record.writes / timeDelta
				ioStatWritesPerSecond := buildDockerMetric("DockerDiskWritesPerSecond", metric.Gauge, writesPerSecond)
				ioStatWritesPerSecond.AddDimension("container_name", record.containerName)
				ioStatWritesPerSecond.AddDimension("container_mount_path", record.containerMountPath)
				ioStatWritesPerSecond.AddDimension("paasta_service", record.paastaService)
				ioStatWritesPerSecond.AddDimension("paasta_instance", record.paastaInstance)
				ioStatWritesPerSecond.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatWritesPerSecond)

				readBytePerSecond := record.readsByte / timeDelta
				ioStatReadBytePerSecond := buildDockerMetric("DockerDiskReadBytePerSecond", metric.Gauge, readBytePerSecond)
				ioStatReadBytePerSecond.AddDimension("container_name", record.containerName)
				ioStatReadBytePerSecond.AddDimension("container_mount_path", record.containerMountPath)
				ioStatReadBytePerSecond.AddDimension("paasta_service", record.paastaService)
				ioStatReadBytePerSecond.AddDimension("paasta_instance", record.paastaInstance)
				ioStatReadBytePerSecond.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatReadBytePerSecond)

				writeBytePerSecond := record.writesByte / timeDelta
				ioStatWriteBytePerSecond := buildDockerMetric("DockerDiskWriteBytePerSecond", metric.Gauge, writeBytePerSecond)
				ioStatWriteBytePerSecond.AddDimension("container_name", record.containerName)
				ioStatWriteBytePerSecond.AddDimension("container_mount_path", record.containerMountPath)
				ioStatWriteBytePerSecond.AddDimension("paasta_service", record.paastaService)
				ioStatWriteBytePerSecond.AddDimension("paasta_instance", record.paastaInstance)
				ioStatWriteBytePerSecond.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatWriteBytePerSecond)

				averageQueueLength := record.ioMillisecondsWeighted / timeDelta / 1000.0
				ioStatAverageQueueLength := buildDockerMetric("DockerDiskAverageQueueLength", metric.Gauge, averageQueueLength)
				ioStatAverageQueueLength.AddDimension("container_name", record.containerName)
				ioStatAverageQueueLength.AddDimension("container_mount_path", record.containerMountPath)
				ioStatAverageQueueLength.AddDimension("paasta_service", record.paastaService)
				ioStatAverageQueueLength.AddDimension("paasta_instance", record.paastaInstance)
				ioStatAverageQueueLength.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatAverageQueueLength)

				utilPercentage := record.ioMilliseconds / timeDelta / 10.0
				ioStatUtilPercentage := buildDockerMetric("DockerDiskUtilPercentage", metric.Gauge, utilPercentage)
				ioStatUtilPercentage.AddDimension("container_name", record.containerName)
				ioStatUtilPercentage.AddDimension("container_mount_path", record.containerMountPath)
				ioStatUtilPercentage.AddDimension("paasta_service", record.paastaService)
				ioStatUtilPercentage.AddDimension("paasta_instance", record.paastaInstance)
				ioStatUtilPercentage.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatUtilPercentage)

				var readAwait float64
				if record.reads > 0 {
					readAwait = record.readsMilliseconds / record.reads
				} else {
					readAwait = 0
				}
				ioStatReadAwait := buildDockerMetric("DockerDiskReadAwait", metric.Gauge, readAwait)
				ioStatReadAwait.AddDimension("container_name", record.containerName)
				ioStatReadAwait.AddDimension("container_mount_path", record.containerMountPath)
				ioStatReadAwait.AddDimension("paasta_service", record.paastaService)
				ioStatReadAwait.AddDimension("paasta_instance", record.paastaInstance)
				ioStatReadAwait.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatReadAwait)

				var writeAwait float64
				if record.writes > 0 {
					writeAwait = record.writesMilliseconds / record.writes
				} else {
					writeAwait = 0
				}
				ioStatWriteAwait := buildDockerMetric("DockerDiskWriteAwait", metric.Gauge, writeAwait)
				ioStatWriteAwait.AddDimension("container_name", record.containerName)
				ioStatWriteAwait.AddDimension("container_mount_path", record.containerMountPath)
				ioStatWriteAwait.AddDimension("paasta_service", record.paastaService)
				ioStatWriteAwait.AddDimension("paasta_instance", record.paastaInstance)
				ioStatWriteAwait.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatWriteAwait)

				var serviceTime, await, averageRequestSizeByte float64
				// Set to zero so the nodes are valid even if we have 0 io for the metric duration
				averageRequestSizeByte = 0
				if io > 0 {
					averageRequestSizeByte = (record.readsByte + record.writesByte) / io
					serviceTime = record.ioMilliseconds / io
					await = (record.readsMilliseconds + record.writesMilliseconds) / io
				} else {
					averageRequestSizeByte = 0
					serviceTime = 0
					await = 0
				}
				ioStatServiceTime := buildDockerMetric("DockerDiskServiceTime", metric.Gauge, serviceTime)
				ioStatServiceTime.AddDimension("container_name", record.containerName)
				ioStatServiceTime.AddDimension("container_mount_path", record.containerMountPath)
				ioStatServiceTime.AddDimension("paasta_service", record.paastaService)
				ioStatServiceTime.AddDimension("paasta_instance", record.paastaInstance)
				ioStatServiceTime.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatServiceTime)

				ioStatAwait := buildDockerMetric("DockerDiskAwait", metric.Gauge, await)
				ioStatAwait.AddDimension("container_name", record.containerName)
				ioStatAwait.AddDimension("container_mount_path", record.containerMountPath)
				ioStatAwait.AddDimension("paasta_service", record.paastaService)
				ioStatAwait.AddDimension("paasta_instance", record.paastaInstance)
				ioStatAwait.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatAwait)

				ioStatAverageRequestSizeByte := buildDockerMetric("DockerDiskAverageRequestSizeByte", metric.Gauge, averageRequestSizeByte)
				ioStatAverageRequestSizeByte.AddDimension("container_name", record.containerName)
				ioStatAverageRequestSizeByte.AddDimension("container_mount_path", record.containerMountPath)
				ioStatAverageRequestSizeByte.AddDimension("paasta_service", record.paastaService)
				ioStatAverageRequestSizeByte.AddDimension("paasta_instance", record.paastaInstance)
				ioStatAverageRequestSizeByte.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatAverageRequestSizeByte)

				iops := io / timeDelta
				ioStatIops := buildDockerMetric("DockerDiskIops", metric.Gauge, iops)
				ioStatIops.AddDimension("container_name", record.containerName)
				ioStatIops.AddDimension("container_mount_path", record.containerMountPath)
				ioStatIops.AddDimension("paasta_service", record.paastaService)
				ioStatIops.AddDimension("paasta_instance", record.paastaInstance)
				ioStatIops.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatIops)

				// http://www.scribd.com/doc/15013525 Page 28
				concurrentIo := (readsPerSecond + writesPerSecond) * (serviceTime / 1000.0)
				ioStatConcurrentIo := buildDockerMetric("DockerDiskConcurrentIO", metric.Gauge, concurrentIo)
				ioStatConcurrentIo.AddDimension("container_name", record.containerName)
				ioStatConcurrentIo.AddDimension("container_mount_path", record.containerMountPath)
				ioStatConcurrentIo.AddDimension("paasta_service", record.paastaService)
				ioStatConcurrentIo.AddDimension("paasta_instance", record.paastaInstance)
				ioStatConcurrentIo.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatConcurrentIo)

				ioInProgress := record.ioInProgress
				ioStatIOInProgress := buildDockerMetric("DockerDiskIOInProgress", metric.Gauge, ioInProgress)
				ioStatIOInProgress.AddDimension("container_name", record.containerName)
				ioStatIOInProgress.AddDimension("container_mount_path", record.containerMountPath)
				ioStatIOInProgress.AddDimension("paasta_service", record.paastaService)
				ioStatIOInProgress.AddDimension("paasta_instance", record.paastaInstance)
				ioStatIOInProgress.AddDimension("paasta_cluster", record.paastaCluster)
				ret = append(ret, ioStatIOInProgress)
			}
		}
	}
	return ret
}

func metricsForBlkioStatsEntries(blkioStatsEntries []docker.BlkioStatsEntry, metricNameTemplate string) []metric.Metric {
	ret := []metric.Metric{}
	for _, blkio := range blkioStatsEntries {
		io := buildDockerMetric(fmt.Sprintf(metricNameTemplate, blkio.Op), metric.CumulativeCounter, float64(blkio.Value))
		io.AddDimension("blkdev", fmt.Sprintf("%d:%d", blkio.Major, blkio.Minor))
		ret = append(ret, io)
	}
	return ret
}

// sendMetrics writes all the metrics received to the collector channel.
func (d DockerStats) sendMetrics(metrics []metric.Metric) {
	for _, m := range metrics {
		d.Channel() <- m
	}
}

// Function that extracts additional dimensions from the docker environmental variables set up by the user
// in the configuration file.
func (d DockerStats) extractDimensions(container *docker.Container) map[string]string {
	envVars := container.Config.Env
	ret := map[string]string{}

	for dimension, r := range d.compiledRegex {
		for _, envVariable := range envVars {
			envArray := strings.Split(envVariable, "=")
			if r.tag == envArray[0] {
				subMatch := r.regex.FindStringSubmatch(envArray[1])
				if len(subMatch) > 0 {
					ret[dimension] = strings.Replace(subMatch[len(subMatch)-1], "--", "_", -1)
				}
			}
		}
	}
	d.log.Debug(ret)
	return ret
}

func buildDockerMetric(name string, metricType string, value float64) (m metric.Metric) {
	m = metric.New(name)
	m.MetricType = metricType
	m.Value = value
	return m
}

// Function that compute the current cpu usage percentage combining current and last values.
func calculateCPUPercent(previousCPU, previousSystem uint64, stats *docker.Stats) float64 {
	var (
		cpuPercent = 0.0
		// calculate the change for the cpu usage of the container in between readings
		cpuDelta = float64(stats.CPUStats.CPUUsage.TotalUsage - previousCPU)
		// calculate the change for the entire system between readings
		systemDelta = float64(stats.CPUStats.SystemCPUUsage - previousSystem)
	)

	if systemDelta > 0.0 && cpuDelta > 0.0 {
		cpuPercent = (cpuDelta / systemDelta) * float64(len(stats.CPUStats.CPUUsage.PercpuUsage)) * 100.0
	}
	return cpuPercent
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// returns a map, each record containing (device name --> [disk stats])
func (d DockerStats) ObtainDiskStats() (map[string][]string, error) {
	devNameMinMajMap := make(map[string][]string)

	file, err := os.Open("/proc/diskstats")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// read the contents of the file with a reader.
	reader := bufio.NewReader(file)

	// read line-by-line
	var line string
	for {
		line, err = reader.ReadString('\n')

		if err != nil {
			break
		}

		// split the line on space
		rec := strings.Fields(line)
		if len(rec) == 14 {
			if !strings.HasPrefix(rec[2], "ram") && !strings.HasPrefix(rec[2], "loop") {
				devNameMinMajMap[rec[2]] = []string{rec[0], rec[1], rec[2], rec[3], rec[4], rec[5], rec[6], rec[7], rec[8], rec[9], rec[10], rec[11], rec[12], rec[13]}
			}
		} else {
			d.log.Warning("This record of /proc/diskstats does not contain the required number of fields: " + line + " Shall not be processed.")
		}
	}

	if err != io.EOF {
		d.log.Error("Failed!: ", err)
		return nil, err
	}

	return devNameMinMajMap, nil
}

// returns a collection of records each having (device name, major, minor, mount path, reads, writes)
func (d DockerStats) ObtainDiskIOStats(diskStats map[string][]string) ([]DiskIOStats, error) {
	var diskIOStatsList []DiskIOStats
	var major int
	var minor int
	var reads, readsMerged, readsSectors, readsMilliseconds, writes, writesMerged, writesSectors, writesMilliseconds, ioInProgress, ioMilliseconds, ioMillisecondsWeighted float64

	file, err := os.Open("/proc/mounts")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// read contents of the file
	reader := bufio.NewReader(file)

	// read line-by-line
	var line string
	for {
		line, err = reader.ReadString('\n')

		if err != nil {
			break
		}

		// split the line on space
		values := strings.Fields(line)
		if len(values) < 2 {
			d.log.Error("This record of /proc/mounts does not contain the required number of fields: " + line + " Shall not be processed.")
			continue
		}
		deviceName := values[0]

		// since we could either find an exact match of the device name or /dev/ appended to it in /proc/mounts
		if strings.HasPrefix(deviceName, "/") {
			// extract the value after the last slash, since that should be the device name
			splitOnSlash := strings.Split(values[0], "/")
			deviceName = splitOnSlash[len(splitOnSlash)-1]
		}
		// check if the deviceName is present in the devNameMinMaj map
		if stats, ok := diskStats[deviceName]; ok {
			// extract the major and minor values
			major, err = strconv.Atoi(stats[0])
			if err != nil {
				d.log.Warning("Could not parse " + stats[0] + " to an integer. Skipping this line.")
				continue
			}
			minor, err = strconv.Atoi(stats[1])
			if err != nil {
				d.log.Warning("Could not parse " + stats[1] + " to an integer. Skipping this line.")
				continue
			}
			reads, err = strconv.ParseFloat(stats[3], 64)
			if err != nil {
				d.log.Warning("Could not parse " + stats[3] + " to float. Skipping this line.")
				continue
			}
			readsMerged, err = strconv.ParseFloat(stats[4], 64)
			if err != nil {
				d.log.Warning("Could not parse " + stats[4] + " to float. Skipping this line.")
				continue
			}
			readsSectors, err = strconv.ParseFloat(stats[5], 64)
			if err != nil {
				d.log.Warning("Could not parse " + stats[5] + " to float. Skipping this line.")
				continue
			}
			readsMilliseconds, err = strconv.ParseFloat(stats[6], 64)
			if err != nil {
				d.log.Warning("Could not parse " + stats[6] + " to float. Skipping this line.")
				continue
			}
			writes, err = strconv.ParseFloat(stats[7], 64)
			if err != nil {
				d.log.Warning("Could not parse " + stats[4] + " to float. Skipping this line.")
				continue
			}
			writesMerged, err = strconv.ParseFloat(stats[8], 64)
			if err != nil {
				d.log.Warning("Could not parse " + stats[8] + " to float. Skipping this line.")
				continue
			}
			writesSectors, err = strconv.ParseFloat(stats[9], 64)
			if err != nil {
				d.log.Warning("Could not parse " + stats[9] + " to float. Skipping this line.")
				continue
			}
			writesMilliseconds, err = strconv.ParseFloat(stats[10], 64)
			if err != nil {
				d.log.Warning("Could not parse " + stats[10] + " to float. Skipping this line.")
				continue
			}
			ioInProgress, err = strconv.ParseFloat(stats[11], 64)
			if err != nil {
				d.log.Warning("Could not parse " + stats[11] + " to float. Skipping this line.")
				continue
			}
			ioMilliseconds, err = strconv.ParseFloat(stats[12], 64)
			if err != nil {
				d.log.Warning("Could not parse " + stats[12] + " to float. Skipping this line.")
				continue
			}
			ioMillisecondsWeighted, err = strconv.ParseFloat(stats[13], 64)
			if err != nil {
				d.log.Warning("Could not parse " + stats[13] + " to float. Skipping this line.")
				continue
			}
			// create a deviceStats struct object and append to the deviceStatsList
			diskIOStatsList = append(diskIOStatsList, DiskIOStats{deviceName, major, minor, values[1], reads, readsMerged, readsSectors, readsMilliseconds, writes, writesMerged, writesSectors, writesMilliseconds, ioInProgress, ioMilliseconds, ioMillisecondsWeighted})

		}
	}
	if err != io.EOF {
		d.log.Error("Failed!: ", err)
		return nil, err
	}
	return diskIOStatsList, err
}

// returns a list of DiskIOPaastaStats for the given container
func (d DockerStats) ObtainDiskIOAndPaastaStats(container *docker.Container, diskIOStatsList []DiskIOStats, realPathGetterFunc RealPathGetter) []DiskIOPaastaStats {
	var paastaIOStatsList []DiskIOPaastaStats
	var deviceMountPath, containerName string
	var err error
	var reads, readsMerged, readsByte, readsMilliseconds, writes, writesMerged, writesByte, writesMilliseconds, ioInProgress, ioMilliseconds, ioMillisecondsWeighted float64
	var readsKey, readsMergedKey, readsByteKey, readsMillisecondsKey, writesKey, writesMergedKey, writesByteKey, writesMillisecondsKey, ioMillisecondsKey, ioMillisecondsWeightedKey string

	// check all the mounts of the container to check if it matches the mount paths of devices on this device.
	for _, mount := range container.Mounts {
		mountPath := mount.Source
		for _, device := range diskIOStatsList {
			envVariableMap := make(map[string]string)
			deviceMountPath = device.mountPath
			// to elimiate any mismatch due to the symlink /var/lib for /ephemeral
			deviceMountPath, err = realPathGetterFunc(deviceMountPath)
			// if realPathGetter fails, manually check if there is a symlink and replace ephemeral --> var/lib
			if err != nil {
				if strings.HasPrefix(deviceMountPath, "/ephemeral") {
					deviceMountPath = strings.Replace(deviceMountPath, "ephemeral", "var/lib", 1)
				}
			}

			if mountPath != deviceMountPath {
				continue
			}
			env := container.Config.Env
			// extract the paasta information from the container evn config
			for _, variable := range env {
				if strings.HasPrefix(variable, "PAASTA") {
					name := strings.Split(variable, "=")[0]
					value := strings.Split(variable, "=")[1]
					if (name == "PAASTA_CLUSTER" || name == "PAASTA_INSTANCE" || name == "PAASTA_SERVICE") && len(strings.TrimSpace(value)) > 0 {
						envVariableMap[name] = value
					}
				}
			}
			// we want to emit the complete set of paasta service+cluster+instence or nothing
			if len(envVariableMap) == 3 {
				// extract the container name from the labels
				labels := container.Config.Labels
				containerName = labels["io.kubernetes.container.name"]
				// create distinct keys for each metric of each container, to be able to use the last value in the derivative
				readsKey = BuildKey(containerName, device.deviceName, envVariableMap["PAASTA_SERVICE"], envVariableMap["PAASTA_INSTANCE"], envVariableMap["PAASTA_CLUSTER"], "reads")
				readsMergedKey = BuildKey(containerName, device.deviceName, envVariableMap["PAASTA_SERVICE"], envVariableMap["PAASTA_INSTANCE"], envVariableMap["PAASTA_CLUSTER"], "readsMerged")
				readsByteKey = BuildKey(containerName, device.deviceName, envVariableMap["PAASTA_SERVICE"], envVariableMap["PAASTA_INSTANCE"], envVariableMap["PAASTA_CLUSTER"], "readsByte")
				readsMillisecondsKey = BuildKey(containerName, device.deviceName, envVariableMap["PAASTA_SERVICE"], envVariableMap["PAASTA_INSTANCE"], envVariableMap["PAASTA_CLUSTER"], "readsMillisecondsKey")
				writesKey = BuildKey(containerName, device.deviceName, envVariableMap["PAASTA_SERVICE"], envVariableMap["PAASTA_INSTANCE"], envVariableMap["PAASTA_CLUSTER"], "writes")
				writesMergedKey = BuildKey(containerName, device.deviceName, envVariableMap["PAASTA_SERVICE"], envVariableMap["PAASTA_INSTANCE"], envVariableMap["PAASTA_CLUSTER"], "writesMerged")
				writesByteKey = BuildKey(containerName, device.deviceName, envVariableMap["PAASTA_SERVICE"], envVariableMap["PAASTA_INSTANCE"], envVariableMap["PAASTA_CLUSTER"], "writesByte")
				writesMillisecondsKey = BuildKey(containerName, device.deviceName, envVariableMap["PAASTA_SERVICE"], envVariableMap["PAASTA_INSTANCE"], envVariableMap["PAASTA_CLUSTER"], "writesMilliseconds")
				ioMillisecondsKey = BuildKey(containerName, device.deviceName, envVariableMap["PAASTA_SERVICE"], envVariableMap["PAASTA_INSTANCE"], envVariableMap["PAASTA_CLUSTER"], "ioMilliseconds")
				ioMillisecondsWeightedKey = BuildKey(containerName, device.deviceName, envVariableMap["PAASTA_SERVICE"], envVariableMap["PAASTA_INSTANCE"], envVariableMap["PAASTA_CLUSTER"], "ioMillisecondsWeighted")

				// the max values for read sectors and write sectors depends on the platfrom architecture. Reference : https://gist.github.com/asukakenji/f15ba7e588ac42795f421b48b8aede63
				platformArch := runtime.GOARCH
				if platformArch == "amd64" || platformArch == "arm64" || platformArch == "arm64be" || platformArch == "ppc64" || platformArch == "ppc64le" || platformArch == "mips64" || platformArch == "mips64le" || platformArch == "s390x" || platformArch == "sparc64" {
					// if the platform architecture is 64 bit, use 2**64 -1 else use 2**32 - 1
					d.maxValues["readsSectors"] = 1.8446744e+19
					d.maxValues["writesSectors"] = 1.8446744e+19
				} else {
					d.maxValues["readsSectors"] = 4294967295
					d.maxValues["writesSectors"] = 4294967295
				}

				// each sector has 512 bytes, and the byte unit is byte
				readsByte = d.Derivative(d.maxValues["readsSectors"], readsByteKey, device.readsSectors*sector_size)
				writesByte = d.Derivative(d.maxValues["writesSectors"], writesByteKey, device.writesSectors*sector_size)

				// obtain the derivative for all the metrics
				reads = d.Derivative(d.maxValues["reads"], readsKey, device.reads)
				readsMerged = d.Derivative(d.maxValues["readsMerged"], readsMergedKey, device.readsMerged)
				readsMilliseconds = d.Derivative(d.maxValues["readsMilliseconds"], readsMillisecondsKey, device.readsMilliseconds)
				writes = d.Derivative(d.maxValues["writes"], writesKey, device.writes)
				writesMerged = d.Derivative(d.maxValues["writesMerged"], writesMergedKey, device.writesMerged)
				writesMilliseconds = d.Derivative(d.maxValues["writesMilliseconds"], writesMillisecondsKey, device.writesMilliseconds)
				ioInProgress = device.ioInProgress
				ioMilliseconds = d.Derivative(d.maxValues["ioMilliseconds"], ioMillisecondsKey, device.ioMilliseconds)
				ioMillisecondsWeighted = d.Derivative(d.maxValues["ioMillisecondsWeighted"], ioMillisecondsWeightedKey, device.ioMillisecondsWeighted)

				paastaIOStatsList = append(paastaIOStatsList, DiskIOPaastaStats{containerName, device.deviceName, envVariableMap["PAASTA_SERVICE"], envVariableMap["PAASTA_INSTANCE"], envVariableMap["PAASTA_CLUSTER"], reads, readsMerged, readsByte, readsMilliseconds, writes, writesMerged, writesByte, writesMilliseconds, ioInProgress, ioMilliseconds, ioMillisecondsWeighted, mount.Destination})
				break
			}
		}
	}
	return paastaIOStatsList
}

func BuildKey(containerName string, deviceName string, serviceName string, instanceName string, clusterName string, metricName string) string {
	return containerName + "." + deviceName + "." + serviceName + "." + instanceName + "." + clusterName + "." + metricName
}

func ObtainRealPath(mountPath string) (string, error) {
	mountPathReal, err := realpath.Realpath(mountPath)
	if err == nil {
		return mountPathReal, err
	}
	// return back the string, so that it can be checked for symlink by the error handling code
	return mountPath, err
}

// Calculate the derivative value of the given metric
func (d DockerStats) Derivative(maxValue float64, key string, newValue float64) float64 {
	var derivativeX, derivativeY, result float64
	// calculate the derivative of the metric
	if oldValue, ok := d.lastValues[key]; ok {
		// Check for rollover
		if newValue < oldValue {
			oldValue = oldValue - maxValue
		}
		// Get Change in X (value)
		derivativeX = newValue - oldValue

		derivativeY = 1
		result = float64(derivativeX) / float64(derivativeY)
		if result < 0 {
			result = 0
		}
	} else {
		result = 0
	}

	d.lastValues[key] = newValue

	return result
}
