package collector

import (
	"fullerite/metric"
	"fullerite/util"

	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func getNerveConfigForTest() []byte {
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

func getArtificialUWSGIWorkerStatsResponse() string {
	return `{
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
	}`
}

func getRealUWSGIWorkerStatsResponse() string {
	return `{
	"version":"XXXXXXXXXXXX",
	"listen_queue":0,
	"listen_queue_errors":0,
	"signal_queue":0,
	"load":0,
	"pid":104912,
	"uid":33,
	"gid":33,
	"cwd":"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
	"locks":[
	{
		"user 0":0
	},
	{
		"signal":0
	},
	{
		"filemon":0
	},
	{
		"timer":0
	},
	{
		"rbtimer":0
	},
	{
		"cron":0
	},
	{
		"rpc":0
	},
	{
		"snmp":0
	},
	{
		"cache_healthcheck":0
	}
	],
	"caches":[
	{
		"name":"healthcheck",
		"hash":"djb33x",
		"hashsize":65536,
		"keysize":2048,
		"max_items":32,
		"blocks":32,
		"blocksize":65536,
		"items":0,
		"hits":0,
		"miss":0,
		"full":0,
		"last_modified_at":0
	}
	],
	"sockets":[
	{
		"name":"127.0.0.1:2000",
		"proto":"uwsgi",
		"queue":0,
		"max_queue":100,
		"shared":0,
		"can_offload":0
	}
	],
	"workers":[
	{
		"id":1,
		"pid":79545,
		"accepting":1,
		"requests":1895,
		"delta_requests":345,
		"exceptions":0,
		"harakiri_count":0,
		"signals":300,
		"signal_queue":0,
		"status":"idle",
		"rss":789790720,
		"vsz":1477410816,
		"running_time":19757779,
		"last_spawn":1477079439,
		"respawn_count":5,
		"tx":2244564,
		"avg_rt":182085,
		"apps":[
		{
			"id":0,
			"modifier1":0,
			"mountpoint":"",
			"startup_time":23,
			"requests":1895,
			"exceptions":0,
			"chdir":""
		}
		],
		"cores":[
		{
			"id":0,
			"requests":1895,
			"static_requests":0,
			"routed_requests":0,
			"offloaded_requests":0,
			"write_errors":0,
			"read_errors":0,
			"in_request":0,
			"vars":[

			],
			"req_info":
			{

			}
		}
		]
	},
	{
		"id":2,
		"pid":3930,
		"accepting":1,
		"requests":2419,
		"delta_requests":290,
		"exceptions":0,
		"harakiri_count":0,
		"signals":300,
		"signal_queue":0,
		"status":"busy",
		"rss":827023360,
		"vsz":1514618880,
		"running_time":36199811,
		"last_spawn":1477081120,
		"respawn_count":5,
		"tx":6627598,
		"avg_rt":178958,
		"apps":[
		{
			"id":0,
			"modifier1":0,
			"mountpoint":"",
			"startup_time":23,
			"requests":2420,
			"exceptions":0,
			"chdir":""
		}
		],
		"cores":[
		{
			"id":0,
			"requests":2419,
			"static_requests":0,
			"routed_requests":0,
			"offloaded_requests":0,
			"write_errors":0,
			"read_errors":0,
			"in_request":1,
			"vars":[
"SCRIPT_URL=/",
"SCRIPT_URI=XXXXXXXXXXXXXXXXXXXXX",
"PATH_INFO=/",
"HTTP_USER_AGENT=XXXXXXXXXXXXXXXXXXXXXXX",
"HTTP_ACCEPT=*/*",
"HTTP_HOST=XXXXXXXXXXXXXXXXXXXXXXXX",
"HTTP_X_MODE=ro",
"HTTP_X_FORWARDED_FOR=XXXXXXXXXXXXXXXXXXXXX",
"HTTP_CONNECTION=close",
"PATH=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
"SERVER_SIGNATURE=",
"SERVER_SOFTWARE=XXXXXXXXXXXXXXXXXXXXXXXXXXXX",
"SERVER_NAME=XXXXXXXXXXXX",
"SERVER_ADDR=XXXXXXXXXXXXXXXXXX",
"SERVER_PORT=80",
"REMOTE_ADDR=XXXXXXXXXXXXXXXX",
"DOCUMENT_ROOT=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
"SERVER_ADMIN=XXXXXXXXXXXXXXXXX",
"SCRIPT_FILENAME=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
"REMOTE_PORT=XXXXX",
"GATEWAY_INTERFACE=CGI/1.1",
"SERVER_PROTOCOL=HTTP/1.1",
"REQUEST_METHOD=GET",
"QUERY_STRING=",
"REQUEST_URI=XXXXXXXXXX",
"SCRIPT_NAME=",
"BODY_SIZE=0",
"BODY_SIZE=0",
""
			],
			"req_info":
			{
			"request_start":1477081161
			}
		}
		]
	}
	]
	}
	`
}

func validateUWSGIWorkerStatsResults(t *testing.T, actual []metric.Metric, expectedLength int, results []float64) {
	assert.Equal(t, expectedLength, len(actual))

	for _, m := range actual {

		switch m.Name {
		case "IdleWorkers":
			assert.Equal(t, results[0], m.Value)
			assert.Equal(t, metric.Gauge, m.MetricType)
		case "BusyWorkers":
			assert.Equal(t, results[1], m.Value)
			assert.Equal(t, metric.Gauge, m.MetricType)
		case "SigWorkers":
			assert.Equal(t, results[2], m.Value)
			assert.Equal(t, metric.Gauge, m.MetricType)
		case "CheapWorkers":
			assert.Equal(t, results[3], m.Value)
			assert.Equal(t, metric.Gauge, m.MetricType)
		case "PauseWorkers":
			assert.Equal(t, results[4], m.Value)
			assert.Equal(t, metric.Gauge, m.MetricType)
		case "InvalidWorkers":
			assert.Equal(t, results[5], m.Value)
			assert.Equal(t, metric.Gauge, m.MetricType)
		case "Cheap255Workers":
			assert.Equal(t, results[6], m.Value)
			assert.Equal(t, metric.Gauge, m.MetricType)
		default:
			t.Fatal("Unexpected metric name: " + m.Name)
		}
	}
}

func validateStatsDimensions(t *testing.T, actual []metric.Metric, serviceName, port string) {
	for _, m := range actual {
		assert.Equal(t, 2, len(m.Dimensions))

		val, exists := m.GetDimensionValue("service")
		assert.True(t, exists)
		assert.Equal(t, serviceName, val)

		val, exists = m.GetDimensionValue("port")
		assert.True(t, exists)
		assert.Equal(t, port, val)
	}

}

func validateStatsEmptyChannel(t *testing.T, c chan metric.Metric) {
	close(c)
	for m := range c {
		t.Fatal("The channel was not empty! got value ", m)
	}
}

func getTestNerveUWSGIWorkerStats() *uWSGINerveWorkerStatsCollector {
	return newUWSGINerveWorkerStats(make(chan metric.Metric), 12, l.WithField("testing", "nerveuwsgistats")).(*uWSGINerveWorkerStatsCollector)
}

func TestDefaultConfigNerveUWSGIWorkerStats(t *testing.T) {
	inst := getTestNerveUWSGIWorkerStats()
	inst.Configure(make(map[string]interface{}))

	assert.Equal(t, 12, inst.Interval())
	assert.Equal(t, 2, inst.timeout)
	assert.Equal(t, "/etc/nerve/nerve.conf.json", inst.configFilePath)
	assert.Equal(t, "status/uwsgi", inst.queryPath)
}

func TestConfigNerveUWSGIWorkerStats(t *testing.T) {
	cfg := map[string]interface{}{
		"interval":       345,
		"configFilePath": "/etc/your/moms/house",
		"queryPath":      "littlepiggies",
		"http_timeout":   12,
	}

	inst := getTestNerveUWSGIWorkerStats()
	inst.Configure(cfg)

	assert.Equal(t, 345, inst.Interval())
	assert.Equal(t, "/etc/your/moms/house", inst.configFilePath)
	assert.Equal(t, "littlepiggies", inst.queryPath)
	assert.Equal(t, 12, inst.timeout)
}

func TestErrorQueryStatsEndpointResponse(t *testing.T) {
	//4xx HTTP status code test
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))
	endpoint := ts.URL + "/status/uwsgi"
	ts.Close()

	headers := make(map[string]string)
	_, _, queryEndpointError := queryEndpoint(endpoint, headers, 10)
	assert.NotNil(t, queryEndpointError)

	//Socket closed test
	tsClosed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	tsClosed.Close()
	closedEndpoint := tsClosed.URL + "/status/uwsgi"
	_, queryClosedEndpointResponse, queryClosedEndpointError := queryEndpoint(closedEndpoint, headers, 10)
	assert.NotNil(t, queryClosedEndpointError)
	assert.Equal(t, "", queryClosedEndpointResponse)
}

func convertURL(url string) (string, string) {
	parts := strings.Split(url, ":")
	ip := strings.Replace(parts[1], "/", "", -1)
	port := parts[2]
	return ip, port
}

func DoTesting(t *testing.T, firstResponse string, secondResponse string, results []float64) {
	goodServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, rsp *http.Request) {
		fmt.Fprint(w, firstResponse)
	}))
	defer goodServer.Close()
	// assume format is http://ipaddr:port
	goodIP, goodPort := convertURL(goodServer.URL)
	badIP, badPort, content := "", "", []byte{}

	if secondResponse != "" {
		badServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, rsp *http.Request) {
			if secondResponse == "none" {
				return // no response
			}
			fmt.Fprint(w, secondResponse)
		}))
		defer badServer.Close()
		badIP, badPort = convertURL(badServer.URL)
	}

	if badIP == "" {
		// If whitelisting fails, non_whitelisted_service will send on a closed channel
		minimalNerveConfig := util.CreateMinimalNerveConfig(map[string]util.EndPoint{
			"test_service.things.and.stuff":            util.EndPoint{goodIP, goodPort},
			"non_whitelisted_service.things.and.stuff": util.EndPoint{goodIP, goodPort},
		})
		marshalled, err := json.Marshal(minimalNerveConfig)
		assert.Nil(t, err)
		content = marshalled
	} else {
		minimalNerveConfig := util.CreateMinimalNerveConfig(map[string]util.EndPoint{
			"test_service.things.and.stuff":    util.EndPoint{goodIP, goodPort},
			"other_service.does.lots.of.stuff": util.EndPoint{badIP, badPort},
		})
		marshalled, err := json.Marshal(minimalNerveConfig)
		assert.Nil(t, err)
		content = marshalled
	}

	tmpFile, err := ioutil.TempFile("", "fullerite_testing")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write(content)
	assert.Nil(t, err)

	cfg := map[string]interface{}{
		"configFilePath":    tmpFile.Name(),
		"queryPath":         "",
		"servicesWhitelist": []string{"test_service"},
	}

	inst := getTestNerveUWSGIWorkerStats()
	inst.Configure(cfg)

	go inst.Collect()

	actual := []metric.Metric{}
	length := len(results)
	for i := 0; i < length; i++ {
		actual = append(actual, <-inst.Channel())
	}
	validateUWSGIWorkerStatsResults(t, actual, len(results), results)
	validateStatsDimensions(t, actual, "test_service", goodPort)
	validateStatsEmptyChannel(t, inst.Channel())
}

func TestNerveUWSGIArtificialStatsCollect(t *testing.T) {
	DoTesting(t, getArtificialUWSGIWorkerStatsResponse(), "", []float64{2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0})
}

func TestNerveUWSGIRealStatsCollect(t *testing.T) {
	DoTesting(t, getRealUWSGIWorkerStatsResponse(), "", []float64{1.0, 1.0})
}

func TestNonResponseStatsQueries(t *testing.T) {
	DoTesting(t, getRealUWSGIWorkerStatsResponse(), "none", []float64{1.0, 1.0})
}

func TestInvalidJSONStatsQueries(t *testing.T) {
	DoTesting(t, getArtificialUWSGIWorkerStatsResponse(), "{\"workers\":[{\"a\":\"b\"}]}", []float64{2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0})
}
