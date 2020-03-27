#!/usr/bin/python
# coding=utf-8

import mock
import requests
from test import CollectorTestCase
from test import get_collector_config
from test import unittest

from diamond.collector import Collector
from envoy import EnvoyServerStatsCollector
from envoy import METRICS_TO_COLLECT_TO_TYPE

class TestEnvoyServerStatsCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('EnvoyServerStatsCollector', {
            'interval': 10
        })

        self.collector = EnvoyServerStatsCollector(config, None)

    def test_import(self):
        self.assertTrue(EnvoyServerStatsCollector)

    @mock.patch.object(requests, 'get')
    @mock.patch.object(Collector, 'publish')
    def test(self, publish_mock, requests_get_mock):
        mock_response = mock.Mock()
        mock_response.text = 'foo: 1'
        requests_get_mock.return_value = mock_response
        self.collector.collect()

        metrics = {}
        for metric, _ in METRICS_TO_COLLECT_TO_TYPE:
            metrics['envoy.server.{}'.format(metric)] = 1

        self.assertPublishedMany(publish_mock, metrics)

if __name__ == "__main__":
    unittest.main()
