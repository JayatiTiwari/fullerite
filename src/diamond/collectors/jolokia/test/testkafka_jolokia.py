#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
from test import CollectorTestCase
from test import get_collector_config
from test import unittest

from diamond.collector import Collector
from kafka_jolokia import KafkaJolokiaCollector
from mock import Mock
from mock import patch


def find_metric(metric_list, metric_name):
    return filter(lambda metric: metric["name"].find(metric_name) > -1, metric_list)


def find_by_dimension(metric_list, key, val):
    return filter(lambda metric: metric["dimensions"][key] == val, metric_list)[0]


def list_request(host, port):
    return {'value': {'kafka.server': 'bla'}, 'status': 200}


class TestKafkaJolokiaCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('KafkaJolokiaCollector', {})

        self.collector = KafkaJolokiaCollector(config, None)
        self.collector.list_request = list_request

    def test_import(self):
        self.assertTrue(KafkaJolokiaCollector)

    @patch.object(Collector, 'flush')
    def test_should_create_type(self, publish_mock):
        def se(url):
            return self.getFixture("kafka_server.json")

        patch_urlopen = patch('urllib2.urlopen', Mock(side_effect=se))

        with patch_urlopen:
            self.collector.collect()
            self.assertEquals(len(self.collector.payload), 35)

        metrics = find_metric(self.collector.payload, "kafka.server.BrokerTopicMetrics.MessagesInPerSec.count")
        self.assertNotEqual(len(metrics), 0)
        metric = find_by_dimension(metrics, "topic", "foobar")
        self.assertEquals(metric["type"], "CUMCOUNTER")

        metrics_dots = find_metric(self.collector.payload,
                                   "kafka.server.KafkaServer4.2.BrokerState.value")
        self.assertNotEqual(len(metrics_dots), 0)

    @patch.object(Collector, 'flush')
    def test_blacklisting(self, publish_mock):
        def se(url):
            return self.getFixture("kafka_server.json")

        patch_urlopen = patch('urllib2.urlopen', Mock(side_effect=se))

        with patch_urlopen:
            self.collector.collect()
            self.assertEquals(len(self.collector.payload), 35)

        metrics = find_metric(self.collector.payload, "kafka.server.BrokerTopicMetrics.MessagesInPerSec.meanrate")
        self.assertEquals(len(metrics), 0)

    @patch.object(Collector, 'flush')
    def test_total_topic(self, publish_mock):
        def se(url):
            return self.getFixture("kafka_server.json")

        patch_urlopen = patch('urllib2.urlopen', Mock(side_effect=se))

        with patch_urlopen:
            self.collector.collect()
            self.assertEquals(len(self.collector.payload), 35)

        metrics = find_metric(self.collector.payload, "kafka.server.BrokerTopicMetrics.BytesRejectedPerSec.count")
        self.assertNotEqual(len(metrics), 0)
        metric = find_by_dimension(metrics, "topic", "_TOTAL_")
        self.assertEquals(metric["type"], "CUMCOUNTER")

    @patch.object(Collector, 'flush')
    def test_percentile_metrics(self, publish_mock):
        def se(url):
            return self.getFixture("kafka_server.json")

        patch_urlopen = patch('urllib2.urlopen', Mock(side_effect=se))

        with patch_urlopen:
            self.collector.collect()
            self.assertEquals(len(self.collector.payload), 35)

        metrics = find_metric(self.collector.payload,
                              "kafka.server.FetchRequestAndResponseMetrics.FetchRequestRateAndTimeMs.count")
        self.assertNotEqual(len(metrics), 0)
        metric = metrics[0]
        self.assertEquals(metric["type"], "CUMCOUNTER")

        percentile_metrics = find_metric(self.collector.payload,
                                         "kafka.server.FetchRequestAndResponseMetrics.FetchRequestRateAndTimeMs.75thpercentile")
        self.assertNotEqual(len(percentile_metrics), 0)
        metric = percentile_metrics[0]
        self.assertEquals(metric["type"], "GAUGE")

    @patch.object(Collector, 'flush')
    def test_should_have_kafka_instance_dimension_in_kubernetes_mode(self, flush_mock):
        self.collector.config.update(
            {
                'mode': 'kubernetes',
                'spec': {
                    'dimensions': {
                        'kubernetes': {
                            'kafka_cluster': {
                                'yelp.com/paasta_cluster': '.*'
                            }
                        }
                    },
                    'label_selector': {"yelp.com/paasta_service": "kafka-k8s"}
                }
            })
        self.collector.list_request = list_request
        self.collector.process_config()

        def se(url):
            return self.getFixture("kafka_server.json")

        def kubelet_se():
            return json.loads(self.getFixture('pods.json').getvalue()), None

        with patch('urllib2.urlopen', Mock(side_effect=se)):
            with patch("kubernetes.Kubelet.list_pods", Mock(side_effect=kubelet_se)):
                self.collector.collect()
                self.assertEquals(len(self.collector.payload), 35)

        metrics = find_metric(self.collector.payload,
                              "kafka.server.FetchRequestAndResponseMetrics.FetchRequestRateAndTimeMs.count")
        self.assertNotEqual(len(metrics), 0)
        metric = find_by_dimension(metrics, "kafka_cluster", "infrastage")
        self.assertIsNotNone(metric)


################################################################################
if __name__ == "__main__":
    unittest.main()
