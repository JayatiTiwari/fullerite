package main

import (
	"fullerite/collector"
	"fullerite/config"
	"fullerite/handler"
	"fullerite/metric"
	"sync"

	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var testFakeConfiguration = `{
    "prefix": "test.",
    "interval": 10,
    "defaultDimensions": {
    },

	"collectorsConfigPath": "/tmp",
    "diamondCollectorsPath": "src/diamond/collectors",
    "diamondCollectors": [],

    "collectors": ["FakeCollector","Test"],

    "handlers": {
    }
}
`

var testCollectorConfiguration = `{
	"metricName": "TestMetric",
	"interval": 10
}
`

var (
	tmpTestFakeFile, tempTestCollectorConfig string
)

func TestMain(m *testing.M) {
	logrus.SetLevel(logrus.ErrorLevel)
	if f, err := ioutil.TempFile("/tmp", "fullerite"); err == nil {
		f.WriteString(testFakeConfiguration)
		tmpTestFakeFile = f.Name()
		f.Close()
		defer os.Remove(tmpTestFakeFile)
	}
	if f, err := ioutil.TempFile("/tmp", "fullerite"); err == nil {
		f.WriteString(testCollectorConfiguration)
		tempTestCollectorConfig = f.Name() + ".conf"
		f.Close()
		defer os.Remove(tempTestCollectorConfig)
	}
	os.Exit(m.Run())
}

func TestStartCollectorsEmptyConfig(t *testing.T) {
	logrus.SetLevel(logrus.ErrorLevel)
	collectors := startCollectors(config.Config{})

	assert.NotEqual(t, len(collectors), 1, "should create a Collector")
}

func TestStartCollectorUnknownCollector(t *testing.T) {
	logrus.SetLevel(logrus.ErrorLevel)
	c := make(map[string]interface{})
	collector := startCollector("unknown collector", config.Config{}, c)

	assert.Nil(t, collector, "should NOT create a Collector")
}

func TestStartCollectorsMixedConfig(t *testing.T) {
	logrus.SetLevel(logrus.ErrorLevel)
	conf, _ := config.ReadConfig(tmpTestFakeFile)
	collectors := startCollectors(conf)

	for _, c := range collectors {
		assert.Equal(t, c.Name(), "Test", "Only create valid collectors")
	}
}

func TestStartCollectorTooLong(t *testing.T) {
	logrus.SetLevel(logrus.ErrorLevel)
	c := make(map[string]interface{})
	c["interval"] = 1
	collector := startCollector("Test", config.Config{}, c)
	startMetric := <-collector.Channel()
	assert.Equal(t, "fullerite.begin_collection", startMetric.Name)

	select {
	case m := <-collector.Channel():
		assert.Equal(t, 1.0, m.Value)
		assert.Equal(t, "fullerite.collection_time_exceeded", m.Name)
		assert.Equal(t, metric.Counter, m.MetricType)
		assert.Equal(t, "1", m.Dimensions["interval"])
		return
	case <-time.After(5 * time.Second):
		t.Fail()
	}
}

func TestReadFromCollector(t *testing.T) {
	logrus.SetLevel(logrus.ErrorLevel)
	c := make(map[string]interface{})
	c["interval"] = 1
	collector := collector.New("Test")
	collector.SetInterval(1)
	collector.Configure(c)

	var wg sync.WaitGroup
	wg.Add(2)
	collectorStatChannel := make(chan metric.CollectorEmission)
	go func() {
		defer wg.Done()
		collector.Channel() <- metric.New("hello")
		time.Sleep(time.Duration(2) * time.Second)
		m2 := metric.New("world")
		m2.AddDimension("collectorCanonicalName", "Foobar")
		collector.Channel() <- m2
		time.Sleep(time.Duration(2) * time.Second)
		m3 := metric.New("world")
		m3.AddDimension("collectorCanonicalName", "Foobar")
		collector.Channel() <- m3
		close(collector.Channel())
	}()
	collectorMetrics := map[string]uint64{}
	go func() {
		defer wg.Done()
		for collectorMetric := range collectorStatChannel {
			collectorMetrics[collectorMetric.Name] = collectorMetric.EmissionCount
		}
	}()
	readFromCollector(collector, []handler.Handler{}, collectorStatChannel)
	wg.Wait()
	assert.Equal(t, uint64(1), collectorMetrics["Test"])
	assert.Equal(t, uint64(2), collectorMetrics["Foobar"])
}

func TestCollectorPrefix(t *testing.T) {
	logrus.SetLevel(logrus.ErrorLevel)
	c := make(map[string]interface{})
	c["interval"] = 1
	c["prefix"] = "px."
	collector := collector.New("Test")
	collector.SetInterval(1)
	collector.Configure(c)

	collectorChannel := map[string]handler.CollectorEnd{
		"Test": handler.CollectorEnd{make(chan metric.Metric), 1},
	}

	testHandler := handler.New("Log")
	testHandler.SetCollectorEndpoints(collectorChannel)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		collector.Channel() <- metric.New("hello")
		close(collector.Channel())
	}()
	go func() {
		defer wg.Done()
		testMetric := <-collectorChannel["Test"].Channel
		assert.Equal(t, "px.hello", testMetric.Name)
	}()
	readFromCollector(collector, []handler.Handler{testHandler})
	wg.Wait()
}

func TestCollectorBlacklist(t *testing.T) {
	logrus.SetLevel(logrus.ErrorLevel)

	c := make(map[string]interface{})
	c["interval"] = 1
	c["metrics_blacklist"] = []string{"m[0-9]+$"}
	col := collector.New("Test")
	col.SetInterval(1)
	col.Configure(c)

	var wg sync.WaitGroup
	wg.Add(2)
	collectorStatChannel := make(chan metric.CollectorEmission)

	go func() {
		defer wg.Done()
		col.Channel() <- metric.New("m1")
		time.Sleep(time.Duration(2) * time.Second)
		col.Channel() <- metric.New("m2")
		time.Sleep(time.Duration(2) * time.Second)
		col.Channel() <- metric.New("metric3")
		close(col.Channel())
	}()
	collectorMetrics := map[string]uint64{}
	go func() {
		defer wg.Done()
		for collectorMetric := range collectorStatChannel {
			collectorMetrics[collectorMetric.Name] = collectorMetric.EmissionCount
		}
	}()
	readFromCollector(col, []handler.Handler{}, collectorStatChannel)
	wg.Wait()

	assert.Equal(t, uint64(1), collectorMetrics["Test"])
}
