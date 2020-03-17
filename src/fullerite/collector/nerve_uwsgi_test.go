package collector

import (
	"fullerite/config"
	"fullerite/dropwizard"
	"fullerite/metric"
	"fullerite/util"
	"sort"

	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func init() {
	l.SetLevel(l.DebugLevel)
}

func getTestNerveConfig() []byte {
	raw := `
	{
	    "heartbeat_path": "/var/run/nerve/heartbeat",
	    "instance_id": "srv1-devc",
	    "services": {
	        "example_service.main.norcal-devc.superregion:norcal-devc.13752.new": {
	            "check_interval": 7,
	            "checks": [
	                {
	                    "fall": 2,
	                    "headers": {},
	                    "host": "127.0.0.1",
	                    "open_timeout": 6,
	                    "port": 6666,
	                    "rise": 1,
	                    "timeout": 6,
	                    "type": "http",
	                    "uri": "/http/example_service.main/13752/status"
	                }
	            ],
	            "host": "10.56.5.21",
	            "port": 13752,
	            "weight": 24,
	            "zk_hosts": [
	                "10.40.5.5:22181",
	                "10.40.5.6:22181",
	                "10.40.1.17:22181"
	            ],
	            "zk_path": "/nerve/superregion:norcal-devc/example_service.main"
	        },
	        "example_service.mesosstage_main.norcal-devc.superregion:norcal-devc.13752.new": {
	            "check_interval": 7,
	            "checks": [
	                {
	                    "fall": 2,
	                    "headers": {},
	                    "host": "127.0.0.1",
	                    "open_timeout": 6,
	                    "port": 6666,
	                    "rise": 1,
	                    "timeout": 6,
	                    "type": "http",
	                    "uri": "/http/example_service.mesosstage_main/13752/status"
	                }
	            ],
	            "host": "10.56.5.21",
	            "port": 22222,
	            "weight": 24,
	            "zk_hosts": [
	                "10.40.5.5:22181",
	                "10.40.5.6:22181",
	                "10.40.1.17:22181"
	            ],
	            "zk_path": "/nerve/superregion:norcal-devc/example_service.mesosstage_main"
	        }
	    }
	}
	`
	return []byte(raw)
}

func getTestUWSGIResponse() string {
	return `{
	"counters": {
		"Acounter":{
			"firstrollup": 134,
			"secondrollup": 89
		}
	},
	"meters": {},
	"timers": {
		"some_timer": {
			"average": 123
		},
		"othertimer": {
			"mean": 345
		}
	},
	"gauges": {
		"some_random_metric": {
			"rollup1": 12
		}
	},
	"histograms": {}
	}
	`
}

func getTestJavaResponse() string {
	return `{
	"counters": {
		"Acounter,dim1=val1,dim2=val2,dim3=val3":{
			"count": 100,
			"firstrollup": 134,
			"secondrollup": 89
		}
	},
	"meters": {},
	"timers": {
		"some_timer": {
			"count": 200,
			"value": 123
		},
		"othertimer,dimX=valX": {
			"mean": 345,
			"m1_rate": 3e-323
		}
	},
	"gauges": {
		"some_random_metric": {
			"rollup1": 12
		}
	},
	"histograms": {}
	}
	`
}

func getArtificialSimpleUWSGIWorkerStatsResponse() string {
	return `{
        "workers":[
		{"status":"busy"},
		{"status":"crazy"},
		{"status":"busy"}
	]
	}`
}

func getTestSchemaUWSGIResponse() string {
	return `{
    "service_dims": {
        "firstdim": "first",
        "seconddim": "second"
    },
	"counters": {
		"Acounter":{
			"firstrollup": 134,
			"secondrollup": 89
		}
	},
	"meters": {},
	"timers": {
		"some_timer": {
			"average": 123
		},
		"othertimer": {
			"mean": 345
		}
	},
	"gauges": {
		"some_random_metric": {
			"rollup1": 12
		}
	},
	"histograms": {}
	}
	`
}

func validateUWSGIResults(t *testing.T, actual []metric.Metric, length int) {
	assert.Equal(t, length, len(actual))

	for _, m := range actual {
		switch m.Name {
		case "some_random_metric":
			metricTypeDim, exists := m.GetDimensionValue("type")
			rollup, exists := m.GetDimensionValue("rollup")
			assert.True(t, exists)
			assert.Equal(t, "rollup1", rollup)
			assert.Equal(t, "gauge", metricTypeDim)
			assert.Equal(t, 12.0, m.Value)
			assert.Equal(t, metric.Gauge, m.MetricType)
		case "othertimer":
			metricTypeDim, exists := m.GetDimensionValue("type")
			rollup, exists := m.GetDimensionValue("rollup")
			assert.True(t, exists)
			assert.Equal(t, "mean", rollup)
			assert.Equal(t, "timer", metricTypeDim)
			assert.Equal(t, 345.0, m.Value)
			assert.Equal(t, metric.Gauge, m.MetricType)
		case "some_timer":
			metricTypeDim, exists := m.GetDimensionValue("type")
			rollup, exists := m.GetDimensionValue("rollup")
			assert.True(t, exists)
			assert.Equal(t, "average", rollup)
			assert.Equal(t, "timer", metricTypeDim)
			assert.Equal(t, 123.0, m.Value)
			assert.Equal(t, metric.Gauge, m.MetricType)
		case "Acounter":
			metricTypeDim, exists := m.GetDimensionValue("type")
			rollup, exists := m.GetDimensionValue("rollup")
			assert.True(t, exists)
			assert.Equal(t, metric.Counter, m.MetricType)
			val, exists := map[string]float64{
				"firstrollup":  134,
				"secondrollup": 89,
			}[rollup]
			assert.Equal(t, "counter", metricTypeDim)
			assert.True(t, exists)
			assert.Equal(t, val, m.Value)
		default:
			t.Fatal("Unexpected metric name: " + m.Name)
		}

	}
}

func validateJavaResults(t *testing.T, actual []metric.Metric, serviceName string, servicePort string) {
	assert.Equal(t, 8, len(actual))

	for _, m := range actual {
		metricTypeDim, exists := m.GetDimensionValue("type")
		assert.True(t, exists)
		rollup, exists := m.GetDimensionValue("rollup")
		assert.True(t, exists)

		switch m.Name {
		case "some_random_metric":
			assert.Equal(t, "rollup1", rollup)
			assert.Equal(t, "gauge", metricTypeDim)
			assert.Equal(t, 12.0, m.Value)
			assert.Equal(t, metric.Gauge, m.MetricType)
		case "othertimer":
			val, exists := map[string]float64{
				"mean":    345.0,
				"m1_rate": 3e-323,
			}[rollup]
			assert.Equal(t, "timer", metricTypeDim)
			assert.True(t, exists)
			assert.Equal(t, metric.Gauge, m.MetricType)
			assert.Equal(t, val, m.Value)
			dim, exists := m.GetDimensionValue("dimX")
			assert.True(t, exists)
			assert.Equal(t, "valX", dim)
		case "some_timer":
			val, exists := map[string]float64{
				"value": 123,
				"count": 200,
			}[rollup]
			assert.Equal(t, "timer", metricTypeDim)
			assert.True(t, exists)
			assert.Equal(t, metric.Gauge, m.MetricType)
			assert.Equal(t, val, m.Value)
		case "Acounter":
			assert.Equal(t, metric.Counter, m.MetricType)
			val, exists := map[string]float64{
				"firstrollup":  134,
				"secondrollup": 89,
				"count":        100,
			}[rollup]
			assert.Equal(t, "counter", metricTypeDim)
			assert.True(t, exists)
			assert.Equal(t, val, m.Value)
			dim, exists := m.GetDimensionValue("dim1")
			assert.True(t, exists)
			assert.Equal(t, "val1", dim)
			dim, exists = m.GetDimensionValue("dim2")
			assert.True(t, exists)
			assert.Equal(t, "val2", dim)
			dim, exists = m.GetDimensionValue("dim3")
			assert.True(t, exists)
			assert.Equal(t, "val3", dim)
		default:
			t.Fatal("Unexpected metric name: " + m.Name)
		}

		dim, exists := m.GetDimensionValue("service")
		assert.True(t, exists)
		assert.Equal(t, serviceName, dim)

		val, exists := m.GetDimensionValue("port")
		assert.True(t, exists)
		assert.Equal(t, servicePort, val)
	}
}

func validateFullDimensions(t *testing.T, actual []metric.Metric, serviceName, port string) {
	for _, m := range actual {
		assert.Equal(t, 4, len(m.Dimensions))

		val, exists := m.GetDimensionValue("service")
		assert.True(t, exists)
		assert.Equal(t, serviceName, val)

		val, exists = m.GetDimensionValue("port")
		assert.True(t, exists)
		assert.Equal(t, port, val)
	}

}

func validateFullSchemaDimensions(t *testing.T, actual []metric.Metric, serviceName, port string) {
	for _, m := range actual {
		assert.Equal(t, 6, len(m.Dimensions))

		val, exists := m.GetDimensionValue("service")
		assert.True(t, exists)
		assert.Equal(t, serviceName, val)

		val, exists = m.GetDimensionValue("port")
		assert.True(t, exists)
		assert.Equal(t, port, val)

		val, exists = m.GetDimensionValue("firstdim")
		assert.True(t, exists)
		assert.Equal(t, "first", val)

		val, exists = m.GetDimensionValue("seconddim")
		assert.True(t, exists)
		assert.Equal(t, "second", val)
	}

}

func validateEmptyChannel(t *testing.T, c chan metric.Metric) {
	close(c)
	for m := range c {
		t.Fatal("The channel was not empty! got value ", m)
	}
}

func parseURL(url string) (string, string) {
	parts := strings.Split(url, ":")
	ip := strings.Replace(parts[1], "/", "", -1)
	port := parts[2]
	return ip, port
}

func getTestNerveUWSGI() *nerveUWSGICollector {
	return newNerveUWSGI(make(chan metric.Metric), 12, l.WithField("testing", "nerveuwsgi")).(*nerveUWSGICollector)
}

func TestDefaultConfigNerveUWSGI(t *testing.T) {
	inst := getTestNerveUWSGI()
	inst.Configure(make(map[string]interface{}))

	assert.Equal(t, 12, inst.Interval())
	assert.Equal(t, 2, inst.timeout)
	assert.Equal(t, "/etc/nerve/nerve.conf.json", inst.configFilePath)
	assert.Equal(t, "status/metrics", inst.queryPath)
	assert.Equal(t, "status/uwsgi", inst.workersStatsQueryPath)
}

func TestConfigNerveUWSGI(t *testing.T) {
	cfg := map[string]interface{}{
		"interval":       345,
		"configFilePath": "/etc/your/moms/house",
		"queryPath":      "littlepiggies",
		"http_timeout":   12,
	}

	inst := getTestNerveUWSGI()
	inst.Configure(cfg)

	assert.Equal(t, 345, inst.Interval())
	assert.Equal(t, "/etc/your/moms/house", inst.configFilePath)
	assert.Equal(t, "littlepiggies", inst.queryPath)
	assert.Equal(t, 12, inst.timeout)
}

func TestConfigNerveUWSGIserviceHeadersMap(t *testing.T) {

	content := []byte(`{
	"interval": 345,
	"configFilePath": "/tmp/nerve/nerve.conf.json",
	"queryPath": "status/metrics",
	"http_timeout": 12,
	"serviceHeaders": {
		"yelp-main": {
			"Host": "internalapi"
		}
	}
}`)
	tmpfile, _ := ioutil.TempFile("", "")
	tmpfile.Write(content)
	tmpfile.Close()
	defer os.Remove(tmpfile.Name()) // clean up

	cfg, _ := config.ReadCollectorConfig(tmpfile.Name())

	inst := getTestNerveUWSGI()
	inst.Configure(cfg)

	assert.Equal(t, map[string]string{"Host": "internalapi"}, inst.serviceHeadersMap["yelp-main"])
}

func TestErrorQueryEndpointResponse(t *testing.T) {
	//4xx HTTP status code test
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))
	endpoint := ts.URL + "/status/metrics"
	ts.Close()

	headers := make(map[string]string)
	_, _, queryEndpointError := queryEndpoint(endpoint, headers, 10)
	assert.NotNil(t, queryEndpointError)

	//Socket closed test
	tsClosed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	tsClosed.Close()
	closedEndpoint := tsClosed.URL + "/status/metrics"
	_, queryClosedEndpointResponse, queryClosedEndpointError := queryEndpoint(closedEndpoint, headers, 10)
	assert.NotNil(t, queryClosedEndpointError)
	assert.Equal(t, "", queryClosedEndpointResponse)

}

func TestNerveUWSGICollect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, rsp *http.Request) {
		fmt.Fprint(w, getTestUWSGIResponse())
	}))
	defer server.Close()

	// assume format is http://ipaddr:port
	ip, port := parseURL(server.URL)
	minimalNerveConfig := util.CreateMinimalNerveConfig(map[string]util.EndPoint{
		"test_service.things.and.stuff": util.EndPoint{ip, port},
	})

	tmpFile, err := ioutil.TempFile("", "fullerite_testing")
	defer os.Remove(tmpFile.Name())
	assert.Nil(t, err)

	marshalled, err := json.Marshal(minimalNerveConfig)
	assert.Nil(t, err)

	_, err = tmpFile.Write(marshalled)
	assert.Nil(t, err)

	cfg := map[string]interface{}{
		"configFilePath": tmpFile.Name(),
		"queryPath":      "",
	}

	inst := getTestNerveUWSGI()
	inst.Configure(cfg)

	go inst.Collect()

	length := 5
	actual := []metric.Metric{}
	for i := 0; i < length; i++ {
		actual = append(actual, <-inst.Channel())
	}

	validateUWSGIResults(t, actual, length)
	validateFullDimensions(t, actual, "test_service", port)
	validateEmptyChannel(t, inst.Channel())
}

func TestNerveUWSGICollectWithSchema(t *testing.T) {
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status/metrics":
			w.Header().Set("Metrics-Schema", "uwsgi.1.1")
			fmt.Fprint(w, getTestSchemaUWSGIResponse())
		default:
			w.WriteHeader(404)
		}
	})
	// Dfault configuration
	cfg := map[string]interface{}{}
	expectedMetrics := []metric.Metric{
		metric.Metric{Name: "some_random_metric", MetricType: "gauge", Value: 12, Dimensions: map[string]string{"type": "gauge", "rollup": "rollup1", "firstdim": "first", "seconddim": "second", "service": "test_service", "port": "OptionalWillBeReplacedByTestFunc"}},
		metric.Metric{Name: "Acounter", MetricType: "counter", Value: 134, Dimensions: map[string]string{"type": "counter", "rollup": "firstrollup", "firstdim": "first", "seconddim": "second", "service": "test_service"}},
		metric.Metric{Name: "Acounter", MetricType: "counter", Value: 89, Dimensions: map[string]string{"type": "counter", "rollup": "secondrollup", "firstdim": "first", "seconddim": "second", "service": "test_service"}},
		metric.Metric{Name: "some_timer", MetricType: "gauge", Value: 123, Dimensions: map[string]string{"type": "timer", "rollup": "average", "firstdim": "first", "seconddim": "second", "service": "test_service"}},
		metric.Metric{Name: "othertimer", MetricType: "gauge", Value: 345, Dimensions: map[string]string{"type": "timer", "rollup": "mean", "firstdim": "first", "seconddim": "second", "service": "test_service"}},
	}
	assertNerveUWSGICollectedMetrics(t, httpHandler, cfg, expectedMetrics)
}

func TestNerveUWSGICollectWorkersStatsDisabled(t *testing.T) {
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status/metrics":
			w.Header().Set("Metrics-Schema", "uwsgi.1.1")
			fmt.Fprint(w, `{
				"service_dims": {"firstdim": "first","seconddim": "second"},
				"meters": {"pyramid_uwsgi_metrics.tweens.2xx-responses":{"count": 987}}
			}`)
		case "/status/uwsgi":
			fmt.Fprint(w, `{"workers":[
				{"status":"busy"}
			]}`)
		}
	})
	cfg := map[string]interface{}{
		"workersStatsEnabled": false,
	}
	expectedMetrics := []metric.Metric{
		metric.Metric{Name: "pyramid_uwsgi_metrics.tweens.2xx-responses", MetricType: "gauge", Value: 987, Dimensions: map[string]string{"type": "meter", "rollup": "count", "firstdim": "first", "seconddim": "second", "service": "test_service"}},
	}
	assertNerveUWSGICollectedMetrics(t, httpHandler, cfg, expectedMetrics)
}
func TestNerveUWSGICollectWorkersStatsEnabledNoUWSGIHeader(t *testing.T) {
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status/metrics":
			w.Header().Set("Metrics-Schema", "java11")
			fmt.Fprint(w, `{
				"service_dims": {"firstdim": "first","seconddim": "second"},
				"meters": {"foo":{"count": 987}}
			}`)
		case "/status/uwsgi":
			fmt.Fprint(w, `{"workers":[
				{"status":"notgonnahappen"}
			]}`)
		}
	})
	cfg := map[string]interface{}{
		"workersStatsEnabled": true,
	}
	expectedMetrics := []metric.Metric{
		metric.Metric{Name: "foo", MetricType: "gauge", Value: 987, Dimensions: map[string]string{"type": "meter", "rollup": "count", "firstdim": "first", "seconddim": "second", "service": "test_service"}},
	}
	assertNerveUWSGICollectedMetrics(t, httpHandler, cfg, expectedMetrics)
}
func TestNerveUWSGICollectWorkersStatsEnabledNoServiceDims(t *testing.T) {
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status/metrics":
			w.Header().Set("Metrics-Schema", "uwsgi.1.1")
			fmt.Fprint(w, `{
				"meters": {"foo":{"count": 987}}
			}`)
		case "/status/uwsgi":
			fmt.Fprint(w, `{"workers":[
				{"status":"busy"}
			]}`)
		}
	})
	cfg := map[string]interface{}{
		"workersStatsEnabled": true,
	}
	expectedMetrics := []metric.Metric{
		metric.Metric{Name: "foo", MetricType: "gauge", Value: 987, Dimensions: map[string]string{"type": "meter", "rollup": "count", "service": "test_service"}},
		metric.Metric{Name: "BusyWorkers", MetricType: "gauge", Value: 1, Dimensions: map[string]string{"service": "test_service"}},
		metric.Metric{Name: "IdleWorkers", MetricType: "gauge", Value: 0, Dimensions: map[string]string{"service": "test_service"}},
	}
	assertNerveUWSGICollectedMetrics(t, httpHandler, cfg, expectedMetrics)
}
func TestNerveUWSGICollectWorkersStatsEnabledServiceDims(t *testing.T) {
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status/metrics":
			w.Header().Set("Metrics-Schema", "uwsgi.1.1")
			fmt.Fprint(w, `{
				"service_dims": {"firstdim": "first","seconddim": "second"},
				"meters": {"foo":{"count": 987}}
			}`)
		case "/status/uwsgi":
			fmt.Fprint(w, `{"workers":[
				{"status":"busy"}
			]}`)
		}
	})
	cfg := map[string]interface{}{
		"workersStatsEnabled": true,
	}
	expectedMetrics := []metric.Metric{
		metric.Metric{Name: "foo", MetricType: "gauge", Value: 987, Dimensions: map[string]string{"type": "meter", "rollup": "count", "firstdim": "first", "seconddim": "second", "service": "test_service"}},
		metric.Metric{Name: "BusyWorkers", MetricType: "gauge", Value: 1, Dimensions: map[string]string{"firstdim": "first", "seconddim": "second", "service": "test_service"}},
		metric.Metric{Name: "IdleWorkers", MetricType: "gauge", Value: 0, Dimensions: map[string]string{"firstdim": "first", "seconddim": "second", "service": "test_service"}},
	}
	assertNerveUWSGICollectedMetrics(t, httpHandler, cfg, expectedMetrics)
}
func TestNerveUWSGICollectWorkersStatsEnabledServiceDimsFullOldPyramid(t *testing.T) {
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status/metrics":
			w.Header().Set("Metrics-Schema", "uwsgi.1.1")
			fmt.Fprint(w, `{
				"version": "1.3.0a1",
				"counters": {},
				"gauges": {},
				"histograms": {},
				"meters": {
				  "pyramid_uwsgi_metrics.tweens.2xx-responses": {
					"count": 91911,
					"m15_rate": 0.2617664902208245,
					"units": "events/second"
				  }
				},
				"timers": {
				  "pyramid_uwsgi_metrics.tweens.status": {
					"count": 80255,
					"p99": 0.8959770202636719,
					"mean_rate": 0.22937415235065844,
					"duration_units": "milliseconds",
					"rate_units": "calls/second"
				  }
				},
				"service_dims": {
				  "service_name": "styleguide",
				  "instance_name": "main"
				}
			  }`)
		case "/status/uwsgi":
			fmt.Fprint(w, `{"workers":[
				{"status":"busy"},
				{"status":"idle"}
			]}`)
		}
	})
	cfg := map[string]interface{}{
		"workersStatsEnabled": true,
	}
	expectedMetrics := []metric.Metric{
		metric.Metric{Name: "BusyWorkers", MetricType: "gauge", Value: 1, Dimensions: map[string]string{"service_name": "styleguide", "instance_name": "main", "service": "test_service"}},
		metric.Metric{Name: "IdleWorkers", MetricType: "gauge", Value: 1, Dimensions: map[string]string{"instance_name": "main", "service_name": "styleguide", "service": "test_service"}},
		metric.Metric{Name: "pyramid_uwsgi_metrics.tweens.2xx-responses", MetricType: "gauge", Value: 91911, Dimensions: map[string]string{"rollup": "count", "type": "meter", "service_name": "styleguide", "instance_name": "main", "service": "test_service"}},
		metric.Metric{Name: "pyramid_uwsgi_metrics.tweens.2xx-responses", MetricType: "gauge", Value: 0.2617664902208245, Dimensions: map[string]string{"service": "test_service", "rollup": "m15_rate", "type": "meter", "service_name": "styleguide", "instance_name": "main"}},
		metric.Metric{Name: "pyramid_uwsgi_metrics.tweens.status", MetricType: "gauge", Value: 0.8959770202636719, Dimensions: map[string]string{"rollup": "p99", "type": "timer", "service_name": "styleguide", "instance_name": "main", "service": "test_service"}},
		metric.Metric{Name: "pyramid_uwsgi_metrics.tweens.status", MetricType: "gauge", Value: 80255, Dimensions: map[string]string{"rollup": "count", "type": "timer", "service_name": "styleguide", "instance_name": "main", "service": "test_service"}},
		metric.Metric{Name: "pyramid_uwsgi_metrics.tweens.status", MetricType: "gauge", Value: 0.22937415235065844, Dimensions: map[string]string{"service": "test_service", "rollup": "mean_rate", "type": "timer", "service_name": "styleguide", "instance_name": "main"}},
	}
	assertNerveUWSGICollectedMetrics(t, httpHandler, cfg, expectedMetrics)
}
func TestNerveUWSGICollectWorkersStatsEnabledServiceDimsNewPyramid(t *testing.T) {
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status/metrics":
			w.Header().Set("Metrics-Schema", "uwsgi.1.1")
			fmt.Fprint(w, `{
				"gauges": [],
				"format": 2,
				"histograms": [],
				"service_dims": {
				  "instance": "a"
				},
				"version": "4.1.2",
				"timers": [],
				"meters": [
				  {
					"count": 23868,
					"name": "pyramid_uwsgi_metrics.tweens.2xx-responses"
				  }
				],
				"counters": []
			  }`)
		case "/status/uwsgi":
			fmt.Fprint(w, `{"workers":[
				{"status":"busy"},
				{"status":"idle"}
			]}`)
		}
	})
	cfg := map[string]interface{}{
		"workersStatsEnabled": true,
	}
	expectedMetrics := []metric.Metric{
		metric.Metric{Name: "BusyWorkers", MetricType: "gauge", Value: 1, Dimensions: map[string]string{"instance": "a", "service": "test_service"}},
		metric.Metric{Name: "IdleWorkers", MetricType: "gauge", Value: 1, Dimensions: map[string]string{"instance": "a", "service": "test_service"}},
		metric.Metric{Name: "pyramid_uwsgi_metrics.tweens.2xx-responses", MetricType: "gauge", Value: 23868, Dimensions: map[string]string{"rollup": "count", "type": "meter", "instance": "a", "service": "test_service"}},
	}
	assertNerveUWSGICollectedMetrics(t, httpHandler, cfg, expectedMetrics)
}
func TestNerveUWSGICollectBadURL(t *testing.T) {
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status/metrics":
			w.Header().Set("Metrics-Schema", "uwsgi.1.1")
			fmt.Fprint(w, `{
				"service_dims": {"firstdim": "first","seconddim": "second"},
				"meters": {"pyramid_uwsgi_metrics.tweens.2xx-responses":{"count": 987}}
			}`)
		case "/status/uwsgi":
			w.WriteHeader(404)
			fmt.Fprint(w, "NOTHING to see here")
		}
	})
	cfg := map[string]interface{}{
		"workersStatsEnabled": true,
	}
	expectedMetrics := []metric.Metric{
		metric.Metric{Name: "pyramid_uwsgi_metrics.tweens.2xx-responses", MetricType: "gauge", Value: 987, Dimensions: map[string]string{"type": "meter", "rollup": "count", "firstdim": "first", "seconddim": "second", "service": "test_service"}},
	}
	assertNerveUWSGICollectedMetrics(t, httpHandler, cfg, expectedMetrics)
}
func TestNerveUWSGICollectWorkersStatsSlowStatsEndpoint(t *testing.T) {
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status/metrics":
			w.Header().Set("Metrics-Schema", "uwsgi.1.1")
			fmt.Fprint(w, `{
				"service_dims": {"firstdim": "first","seconddim": "second"},
				"meters": {"pyramid_uwsgi_metrics.tweens.2xx-responses":{"count": 987}}
			}`)
		case "/status/uwsgi":
			//Simulate slow endpoint
			time.Sleep(3000 * time.Millisecond)
			fmt.Fprint(w, `{"workers":[
				{"status":"busy"}
			]}`)
		}
	})
	cfg := map[string]interface{}{
		"workersStatsEnabled": true,
	}
	expectedMetrics := []metric.Metric{
		metric.Metric{Name: "pyramid_uwsgi_metrics.tweens.2xx-responses", MetricType: "gauge", Value: 987, Dimensions: map[string]string{"type": "meter", "rollup": "count", "firstdim": "first", "seconddim": "second", "service": "test_service"}},
	}
	assertNerveUWSGICollectedMetrics(t, httpHandler, cfg, expectedMetrics)
}
func TestNerveUWSGICollectWorkersStatsBlacklistedService(t *testing.T) {
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status/metrics":
			w.Header().Set("Metrics-Schema", "uwsgi.1.1")
			fmt.Fprint(w, `{
				"service_dims": {"firstdim": "first","seconddim": "second"},
				"meters": {"pyramid_uwsgi_metrics.tweens.2xx-responses":{"count": 987}}
			}`)
		case "/status/uwsgi":
			fmt.Fprint(w, `{"workers":[
				{"status":"busy"}
			]}`)
		}
	})
	cfg := map[string]interface{}{
		"workersStatsEnabled":   true,
		"workersStatsBlacklist": []string{"test_service"},
	}
	expectedMetrics := []metric.Metric{
		metric.Metric{Name: "pyramid_uwsgi_metrics.tweens.2xx-responses", MetricType: "gauge", Value: 987, Dimensions: map[string]string{"type": "meter", "rollup": "count", "firstdim": "first", "seconddim": "second", "service": "test_service"}},
	}
	assertNerveUWSGICollectedMetrics(t, httpHandler, cfg, expectedMetrics)
}
func TestNerveUWSGICollectWithPyramidWorkersStatsEnabledFullExample(t *testing.T) {
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status/metrics":
			w.Header().Set("Metrics-Schema", "uwsgi.1.1")
			fmt.Fprint(w, getTestSchemaUWSGIResponse())
		case "/status/uwsgi":
			fmt.Fprint(w, getArtificialSimpleUWSGIWorkerStatsResponse())
		default:
			w.WriteHeader(404)
		}
	})
	cfg := map[string]interface{}{
		"workersStatsEnabled": true,
	}
	expectedMetrics := []metric.Metric{
		metric.Metric{Name: "some_random_metric", MetricType: "gauge", Value: 12, Dimensions: map[string]string{"type": "gauge", "rollup": "rollup1", "firstdim": "first", "seconddim": "second", "service": "test_service", "port": "OptionalWillBeReplacedByTestFunc"}},
		metric.Metric{Name: "Acounter", MetricType: "counter", Value: 134, Dimensions: map[string]string{"type": "counter", "rollup": "firstrollup", "firstdim": "first", "seconddim": "second", "service": "test_service"}},
		metric.Metric{Name: "Acounter", MetricType: "counter", Value: 89, Dimensions: map[string]string{"type": "counter", "rollup": "secondrollup", "firstdim": "first", "seconddim": "second", "service": "test_service"}},
		metric.Metric{Name: "some_timer", MetricType: "gauge", Value: 123, Dimensions: map[string]string{"type": "timer", "rollup": "average", "firstdim": "first", "seconddim": "second", "service": "test_service"}},
		metric.Metric{Name: "othertimer", MetricType: "gauge", Value: 345, Dimensions: map[string]string{"type": "timer", "rollup": "mean", "firstdim": "first", "seconddim": "second", "service": "test_service"}},
		metric.Metric{Name: "BusyWorkers", MetricType: "gauge", Value: 2, Dimensions: map[string]string{"firstdim": "first", "seconddim": "second", "service": "test_service"}},
		metric.Metric{Name: "CrazyWorkers", MetricType: "gauge", Value: 1, Dimensions: map[string]string{"firstdim": "first", "seconddim": "second", "service": "test_service"}},
		metric.Metric{Name: "IdleWorkers", MetricType: "gauge", Value: 0, Dimensions: map[string]string{"firstdim": "first", "seconddim": "second", "service": "test_service"}},
	}
	assertNerveUWSGICollectedMetrics(t, httpHandler, cfg, expectedMetrics)
}

func TestNerveJavaCollectWithSchema(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, rsp *http.Request) {
		w.Header().Set("Metrics-Schema", "java-1.1")
		fmt.Fprint(w, getTestJavaResponse())
	}))
	defer server.Close()

	// assume format is http://ipaddr:port
	ip, port := parseURL(server.URL)

	minimalNerveConfig := util.CreateMinimalNerveConfig(map[string]util.EndPoint{
		"test_service.things.and.stuff": util.EndPoint{ip, port},
	})

	tmpFile, err := ioutil.TempFile("", "fullerite_testing")
	defer os.Remove(tmpFile.Name())
	assert.Nil(t, err)

	marshalled, err := json.Marshal(minimalNerveConfig)
	assert.Nil(t, err)

	_, err = tmpFile.Write(marshalled)
	assert.Nil(t, err)

	cfg := map[string]interface{}{
		"configFilePath": tmpFile.Name(),
		"queryPath":      "",
	}

	inst := getTestNerveUWSGI()
	inst.Configure(cfg)

	go inst.Collect()

	actual := []metric.Metric{}
	for i := 0; i < 8; i++ {
		actual = append(actual, <-inst.Channel())
	}

	validateJavaResults(t, actual, "test_service", port)
	validateEmptyChannel(t, inst.Channel())
}

func TestNerveJavaCollectWithSchemaCumulativeCountersEnabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, rsp *http.Request) {
		w.Header().Set("Metrics-Schema", "java-1.1")
		fmt.Fprint(w, getTestJavaResponse())
	}))
	defer server.Close()

	// assume format is http://ipaddr:port
	ip, port := parseURL(server.URL)

	minimalNerveConfig := util.CreateMinimalNerveConfig(map[string]util.EndPoint{
		"test_service.namespace": util.EndPoint{ip, port},
	})

	tmpFile, err := ioutil.TempFile("", "fullerite_testing")
	defer os.Remove(tmpFile.Name())
	assert.Nil(t, err)

	marshalled, err := json.Marshal(minimalNerveConfig)
	assert.Nil(t, err)

	_, err = tmpFile.Write(marshalled)
	assert.Nil(t, err)

	cfg := map[string]interface{}{
		"configFilePath":    tmpFile.Name(),
		"queryPath":         "",
		"servicesWhitelist": []string{"test_service"},
	}

	inst := getTestNerveUWSGI()
	inst.Configure(cfg)

	go inst.Collect()

	actual := []metric.Metric{}
	flag := true
	for flag == true {
		select {
		case metric := <-inst.Channel():
			actual = append(actual, metric)
		case <-time.After(2 * time.Second):
			flag = false
			break
		}
	}
	assert.Equal(t, 7, len(actual))

	for _, m := range actual {
		switch m.Name {
		case "Acounter.firstrollup":
			assert.Equal(t, 134.0, m.Value)
		case "Acounter.count":
			assert.Equal(t, 100.0, m.Value)
		case "Acounter.secondrollup":
			assert.Equal(t, 89.0, m.Value)
		case "some_timer":
			assert.Equal(t, 123.0, m.Value)
		case "some_timer.count":
			assert.Equal(t, 200.0, m.Value)
			assert.Equal(t, metric.CumulativeCounter, m.MetricType)
		case "othertimer.mean":
			assert.Equal(t, 345.0, m.Value)
		case "some_random_metric.rollup1":
			assert.Equal(t, 12.0, m.Value)
		default:
			t.Fatalf("unknown metric name %s", m.Name)
		}
	}
}

func TestNonConflictingServiceQueries(t *testing.T) {
	goodServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, rsp *http.Request) {
		fmt.Fprint(w, getTestUWSGIResponse())
	}))
	defer goodServer.Close()

	badServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, rsp *http.Request) {
		return // no response
	}))
	defer badServer.Close()

	goodIP, goodPort := parseURL(goodServer.URL)
	badIP, badPort := parseURL(badServer.URL)
	minimalNerveConfig := util.CreateMinimalNerveConfig(map[string]util.EndPoint{
		"test_service.things.and.stuff":    util.EndPoint{goodIP, goodPort},
		"other_service.does.lots.of.stuff": util.EndPoint{badIP, badPort},
	})

	tmpFile, err := ioutil.TempFile("", "fullerite_testing")
	defer os.Remove(tmpFile.Name())
	assert.Nil(t, err)

	marshalled, err := json.Marshal(minimalNerveConfig)
	assert.Nil(t, err)

	_, err = tmpFile.Write(marshalled)
	assert.Nil(t, err)

	cfg := map[string]interface{}{
		"configFilePath": tmpFile.Name(),
		"queryPath":      "",
	}

	inst := getTestNerveUWSGI()
	inst.Configure(cfg)

	go inst.Collect()

	length := 5
	actual := []metric.Metric{}
	for i := 0; i < length; i++ {
		actual = append(actual, <-inst.Channel())
	}

	validateUWSGIResults(t, actual, length)
	validateFullDimensions(t, actual, "test_service", goodPort)
	validateEmptyChannel(t, inst.Channel())
}

func TestUWSGIResponseConversion(t *testing.T) {
	uwsgiRsp := []byte(getTestUWSGIResponse())

	actual, err := dropwizard.Parse(uwsgiRsp, "", false)

	assert.Nil(t, err)
	validateUWSGIResults(t, actual, 5)
	for _, m := range actual {
		assert.Equal(t, 2, len(m.Dimensions))
	}
}

func TestNerveUWSGICollectWithBlacklist(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, rsp *http.Request) {
		fmt.Fprint(w, getTestUWSGIResponse())
	}))
	defer server.Close()

	// assume format is http://ipaddr:port
	ip, port := parseURL(server.URL)
	minimalNerveConfig := util.CreateMinimalNerveConfig(map[string]util.EndPoint{
		"test_service.blacklist": util.EndPoint{ip, port},
	})

	tmpFile, err := ioutil.TempFile("", "fullerite_testing")
	defer os.Remove(tmpFile.Name())
	assert.Nil(t, err)

	marshalled, err := json.Marshal(minimalNerveConfig)
	assert.Nil(t, err)

	_, err = tmpFile.Write(marshalled)
	assert.Nil(t, err)

	cfg := map[string]interface{}{
		"configFilePath":       tmpFile.Name(),
		"queryPath":            "",
		"dimensions_blacklist": map[string]string{"rollup": "mean"},
	}

	inst := getTestNerveUWSGI()
	inst.Configure(cfg)

	go inst.Collect()

	actual := []metric.Metric{}
	for i := 0; i < 4; i++ {
		actual = append(actual, <-inst.Channel())
	}

	dropped := metric.Metric{
		Name:       "othertimer",
		MetricType: "gauge",
		Value:      345.0,
		Dimensions: map[string]string{
			"rollup":  "mean",
			"type":    "timer",
			"service": "test_service",
			"port":    port}}
	actual = append(actual, dropped)
	validateUWSGIResults(t, actual, 5)
	validateFullDimensions(t, actual, "test_service", port)
	validateEmptyChannel(t, inst.Channel())
}

type By func(p1, p2 *metric.Metric) bool

// In order to compare slices of metrics, we need them to be in the same order
// This is inspired from SortKeys example at https://golang.org/pkg/sort/

// Sort is a method on the function type, By, that sorts the argument slice according to the function.
func (by By) Sort(metrics []metric.Metric) {
	ms := &metricSorter{
		metrics: metrics,
		by:      by, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(ms)
}

type metricSorter struct {
	metrics []metric.Metric
	by      func(m1, m2 *metric.Metric) bool // Closure used in the Less method.
}

// Len is part of sort.Interface.
func (s *metricSorter) Len() int {
	return len(s.metrics)
}

// Swap is part of sort.Interface.
func (s *metricSorter) Swap(i, j int) {
	s.metrics[i], s.metrics[j] = s.metrics[j], s.metrics[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *metricSorter) Less(i, j int) bool {
	return s.by(&s.metrics[i], &s.metrics[j])
}

func assertNerveUWSGICollectedMetrics(t *testing.T, httpHandler http.HandlerFunc, cfg map[string]interface{}, expectedMetrics []metric.Metric) {
	server := httptest.NewServer(httpHandler)
	defer server.Close()

	// assume format is http://ipaddr:port
	ip, port := parseURL(server.URL)

	minimalNerveConfig := util.CreateMinimalNerveConfig(map[string]util.EndPoint{
		"test_service.things.and.stuff": util.EndPoint{ip, port},
	})

	tmpFile, err := ioutil.TempFile("", "fullerite_testing")
	defer os.Remove(tmpFile.Name())
	assert.Nil(t, err)

	marshalled, err := json.Marshal(minimalNerveConfig)
	assert.Nil(t, err)

	_, err = tmpFile.Write(marshalled)
	assert.Nil(t, err)

	cfg["configFilePath"] = tmpFile.Name()

	inst := getTestNerveUWSGI()
	inst.Configure(cfg)

	go inst.Collect()

	actual := []metric.Metric{}
	length := len(expectedMetrics)
	for i := 0; i < length; i++ {
		actual = append(actual, <-inst.Channel())
	}

	validateEmptyChannel(t, inst.Channel())
	metric.AddToAll(&expectedMetrics, map[string]string{
		"port": port,
	})

	//Extra bit to sort by Value and by Name the slice of metric before assert
	name := func(p1, p2 *metric.Metric) bool {
		return p1.Name < p2.Name
	}
	value := func(p1, p2 *metric.Metric) bool {
		return p1.Value < p2.Value
	}
	By(value).Sort(actual)
	By(name).Sort(actual)
	By(value).Sort(expectedMetrics)
	By(name).Sort(expectedMetrics)
	assert.Equal(t, expectedMetrics, actual)
}
