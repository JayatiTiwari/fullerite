package beatit

import (
	"fullerite/config"
	"fullerite/handler"
	"fullerite/metric"

	"fmt"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

const (
	name    = "beatit"
	version = "0.6.84"
	desc    = "Stress test fullerite handlers"
)

var log = logrus.WithFields(logrus.Fields{"app": "fullerite"})

func generateMetrics(prefix string, numMetrics, dps int, randomize bool) (metrics []metric.Metric) {
	if randomize {
		rand.Seed(time.Now().Unix())
	}
	if numMetrics > dps {
		numMetrics = dps
	}
	dpsPerMetric := dps / numMetrics
	for i := 0; i < numMetrics; i++ {
		suffix := int64(i)
		if randomize {
			suffix = rand.Int63()
		}
		name := fmt.Sprintf("%s_%d", prefix, suffix)
		for j := 0; j < dpsPerMetric; j++ {
			m := metric.New(name)
			m.Value = rand.Float64()
			m.AddDimension("application", "beatit")
			metrics = append(metrics, m)
		}
	}
	return
}

func sendMetrics(handler handler.Handler, metrics []metric.Metric) {
	for _, m := range metrics {
		handler.Channel() <- m
	}
}

func newHandler(name string, c config.Config, dps int) handler.Handler {
	h := handler.New(name)
	h.SetInterval(1)
	h.SetMaxBufferSize(dps)
	h.Configure(c.Handlers[name])
	return h
}

func initLogrus(ctx *cli.Context) {
	logrus.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: time.RFC822,
		FullTimestamp:   true,
	})

	if level, err := logrus.ParseLevel(ctx.String("log_level")); err == nil {
		logrus.SetLevel(level)
	} else {
		log.Error(err)
		logrus.SetLevel(logrus.InfoLevel)
	}
}

func main() {
	app := cli.NewApp()
	app.Name = name
	app.Version = version
	app.Usage = desc
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "signalfx, s",
			Usage: "Enable SignalFx handler",
		},
		cli.BoolFlag{
			Name:  "graphite, g",
			Usage: "Enable Graphite handler",
		},
		cli.BoolFlag{
			Name:  "datadog, d",
			Usage: "Enable Datadog handler",
		},
		cli.BoolFlag{
			Name:  "wavefront, w",
			Usage: "Enable Wavefront handler",
		},
		cli.IntFlag{
			Name:  "num-metrics",
			Value: 100,
			Usage: "Number of metrics names used per task",
		},
		cli.IntFlag{
			Name:  "num-datapoints, dps",
			Value: 1000,
			Usage: "Number of data points to be generated per task",
		},
		cli.IntFlag{
			Name:  "num-tasks, t",
			Value: 1,
			Usage: "Number of concurrent tasks",
		},
		cli.IntFlag{
			Name:  "time",
			Value: 10,
			Usage: "How long do you want to beat it? (in seconds)",
		},
		cli.StringFlag{
			Name:  "prefix",
			Value: "BeatIt",
			Usage: "Metric name prefix. Default will create metrics like 'BeatIt_12345'",
		},
		cli.BoolFlag{
			Name:  "randomize",
			Usage: "Randomize metric names",
		},
		cli.StringFlag{
			Name:  "config, c",
			Value: "/etc/fullerite.conf",
			Usage: "JSON formatted fullerite configuration file. Only handlers part is used by beatit",
		},
		cli.StringFlag{
			Name:  "log_level, l",
			Value: "info",
			Usage: "Logging level (debug, info, warn, error, fatal, panic)",
		},
	}
	app.Action = start
	app.Run(os.Args)
}

func start(ctx *cli.Context) {
	initLogrus(ctx)
	log.Info("Starting beatit...")

	runtime.GOMAXPROCS(runtime.NumCPU())

	c, err := config.ReadConfig(ctx.String("config"))
	if err != nil {
		return
	}

	var handlers []handler.Handler
	for i := 0; i < ctx.Int("num-tasks"); i++ {
		if ctx.Bool("graphite") {
			h := newHandler("Graphite", c, ctx.Int("num-datapoints"))
			go h.Run()
			handlers = append(handlers, h)
		}
		if ctx.Bool("signalfx") {
			h := newHandler("SignalFx", c, ctx.Int("num-datapoints"))
			go h.Run()
			handlers = append(handlers, h)
		}
		if ctx.Bool("datadog") {
			h := newHandler("Datadog", c, ctx.Int("num-datapoints"))
			go h.Run()
			handlers = append(handlers, h)
		}
		if ctx.Bool("wavefront") {
			h := newHandler("Wavefront", c, ctx.Int("num-datapoints"))
			go h.Run()
			handlers = append(handlers, h)
		}
	}

	t := time.Tick(1 * time.Second)
	count := 0
	for _ = range t {
		if count++; count > ctx.Int("time") {
			os.Exit(0)
		}
		metrics := generateMetrics(ctx.String("prefix"),
			ctx.Int("num-metrics"),
			ctx.Int("num-datapoints"),
			ctx.Bool("randomize"))
		for _, h := range handlers {
			go sendMetrics(h, metrics)
		}
	}
}
