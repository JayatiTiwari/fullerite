#!/usr/bin/python
# coding=utf-8

import mock
import httplib
from test import CollectorTestCase
from test import get_collector_config
from test import unittest

import httplib
from diamond.collector import Collector
from envoy import EnvoyServerStatsCollector
from envoy import METRICS_TO_METRIC_TYPE
from envoy import ADMIN_IP
from envoy import ADMIN_PORT
from envoy import ENDPOINT
from envoy import METHOD


class TestEnvoyServerStatsCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('EnvoyServerStatsCollector', {
            'interval': 10
        })

        self.collector = EnvoyServerStatsCollector(config, None)

    def test_import(self):
        self.assertTrue(EnvoyServerStatsCollector)

    @mock.patch.object(httplib, 'HTTPConnection')
    @mock.patch.object(Collector, 'publish')
    def test(self, publish_mock, http_connection_mock):
        # format the response data to look like the output of envoy stats
        mock_response = mock.Mock()
        mock_envoy_stats = ''
        for name in METRICS_TO_METRIC_TYPE.keys():
            mock_envoy_stats += name + ': 100\n'
        # this `stat` should not be published
        mock_envoy_stats += 'stat.i.dont.care.about: 100\n'
        mock_response.read.return_value = mock_envoy_stats

        mock_connection = mock.Mock()
        mock_connection.getresponse.return_value = mock_response

        http_connection_mock.return_value = mock_connection

        self.collector.collect()

        http_connection_mock.assert_called_once_with(ADMIN_IP, ADMIN_PORT)
        mock_connection.request.assert_called_once_with(METHOD, ENDPOINT)
        published_metrics = {'envoy.' + name : 100 for name in METRICS_TO_METRIC_TYPE.keys()}
        self.assertPublishedMany(publish_mock, published_metrics)

if __name__ == "__main__":
    unittest.main()
