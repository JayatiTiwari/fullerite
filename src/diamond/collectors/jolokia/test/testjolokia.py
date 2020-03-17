#!/usr/bin/python
# coding=utf-8
################################################################################

from test import CollectorTestCase
from test import get_collector_config
from test import unittest

from diamond.collector import Collector
from dimension_reader import CompositeDimensionReader
from test_readers import TestHostReader, TestDimensionReader
from jolokia import JolokiaCollector
from mock import Mock
from mock import patch


################################################################################

def test_hosts():
    return ["10.0.0.1", "10.0.0.2"]


class TestJolokiaCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('JolokiaCollector', {})
        self.collector = JolokiaCollector(config, None)

    def test_import(self):
        self.assertTrue(JolokiaCollector)

    @patch.object(Collector, 'publish')
    def test_should_work_with_real_data(self, publish_mock):
        def se(url):
            if 'http://localhost:8778/jolokia/list' in url:
                return self.getFixture('listing')
            else:
                return self.getFixture('stats')

        patch_urlopen = patch('urllib2.urlopen', Mock(side_effect=se))

        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()

        metrics = self.get_metrics()
        self.setDocExample(collector=self.collector.__class__.__name__,
                           metrics=metrics,
                           defaultpath=self.collector.config['url_path'])
        self.assertPublishedMany(publish_mock, metrics)

    @patch.object(Collector, 'publish')
    def test_should_work_in_mutiple_hosts_mode(self, publish_mock):
        port = self.collector.config['port']
        host_reader = self.collector.host_reader
        hosts = test_hosts()
        self.collector.config['multiple_hosts_mode'] = True
        self.collector.host_reader = TestHostReader(hosts)
        requested_list_urls = []

        def se(url):
            list_urls = [
                'http://%s:%s/jolokia/list' % (host, port)
                for host in hosts
            ]
            if any(_url in url for _url in list_urls):
                requested_list_urls.append(url)
                return self.getFixture('listing')
            else:
                return self.getFixture('stats')

        patch_urlopen = patch('urllib2.urlopen', Mock(side_effect=se))

        with patch_urlopen:
            self.collector.collect()
        self.collector.config['mutiple_hosts_mode'] = False
        self.collector.host_reader = host_reader

        self.assertTrue(all(
            "%s:%s" % (h, port) in requested_list_urls[i]
            for i, h in enumerate(hosts)
        ))

        metrics = self.get_metrics()
        self.setDocExample(collector=self.collector.__class__.__name__,
                           metrics=metrics,
                           defaultpath=self.collector.config['url_path'])
        self.assertPublishedMany(publish_mock, metrics, 2)

    @patch.object(Collector, 'publish')
    def test_real_data_with_rewrite(self, publish_mock):
        def se(url):
            if 'http://localhost:8778/jolokia/list' in url:
                return self.getFixture('listing')
            else:
                return self.getFixture('stats')

        patch_urlopen = patch('urllib2.urlopen', Mock(side_effect=se))

        patch_urlopen.start()
        self.collector.rewrite = {'memoryUsage': 'memUsed', '.*\.init': ''}
        self.collector.collect()
        patch_urlopen.stop()

        rewritemetrics = self.get_metrics_rewrite_test()
        self.assertPublishedMany(publish_mock, rewritemetrics)

    @patch.object(Collector, 'publish')
    def test_should_fail_gracefully(self, publish_mock):
        patch_urlopen = patch('urllib2.urlopen', Mock(return_value=self.getFixture('stats_blank')))

        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()

        self.assertPublishedMany(publish_mock, {})

    @patch.object(Collector, 'publish')
    def test_should_skip_when_mbean_request_fails(self, publish_mock):
        def se(url):
            if 'http://localhost:8778/jolokia/list' in url:
                return self.getFixture('listing_with_bad_mbean')
            elif 'p=read/xxx.bad.package:*' in url:
                return self.getFixture('stats_error')
            else:
                return self.getFixture('stats')

        patch_urlopen = patch('urllib2.urlopen', Mock(side_effect=se))

        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()

        metrics = self.get_metrics()
        self.setDocExample(collector=self.collector.__class__.__name__,
                           metrics=metrics,
                           defaultpath=self.collector.config['url_path'])
        self.assertPublishedMany(publish_mock, metrics)

    @patch.object(Collector, 'publish')
    def test_should_set_custom_dimensions(self, publish_mock):
        def se(url):
            if 'http://localhost:8778/jolokia/list' in url:
                return self.getFixture('listing_with_bad_mbean')
            elif 'p=read/xxx.bad.package:*' in url:
                return self.getFixture('stats_error')
            else:
                return self.getFixture('stats')

        dims = {'localhost': {'dim1': 'v1', 'dim2': 'v2'}}
        self.collector.dimension_reader = TestDimensionReader(dims)
        patch_urlopen = patch('urllib2.urlopen', Mock(side_effect=se))
        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()
        self.collector.dimension_reader = CompositeDimensionReader()

        expected_dims = dims['localhost']
        actual_dims = self.collector.host_custom_dimensions
        self.assertEqual(expected_dims, actual_dims)

    def test_should_escape_jolokia_domains(self):
        domain_with_slash = self.collector.escape_domain('some/domain')
        domain_with_bang = self.collector.escape_domain('some!domain')
        domain_with_quote = self.collector.escape_domain('some"domain')
        self.assertEqual(domain_with_slash, 'some%21/domain')
        self.assertEqual(domain_with_bang, 'some%21%21domain')
        self.assertEqual(domain_with_quote, 'some%21%22domain')

    def get_metrics(self):
        prefix = 'java.lang.name_ParNew.type_GarbageCollector.LastGcInfo'
        return {
            prefix + '.startTime': 14259063,
            prefix + '.id': 219,
            prefix + '.duration': 2,
            prefix + '.memoryUsageBeforeGc.Par_Eden_Space.max': 25165824,
            prefix + '.memoryUsageBeforeGc.Par_Eden_Space.committed': 25165824,
            prefix + '.memoryUsageBeforeGc.Par_Eden_Space.init': 25165824,
            prefix + '.memoryUsageBeforeGc.Par_Eden_Space.used': 25165824,
            prefix + '.memoryUsageBeforeGc.CMS_Old_Gen.max': 73400320,
            prefix + '.memoryUsageBeforeGc.CMS_Old_Gen.committed': 73400320,
            prefix + '.memoryUsageBeforeGc.CMS_Old_Gen.init': 73400320,
            prefix + '.memoryUsageBeforeGc.CMS_Old_Gen.used': 5146840,
            prefix + '.memoryUsageBeforeGc.CMS_Perm_Gen.max': 85983232,
            prefix + '.memoryUsageBeforeGc.CMS_Perm_Gen.committed': 23920640,
            prefix + '.memoryUsageBeforeGc.CMS_Perm_Gen.init': 21757952,
            prefix + '.memoryUsageBeforeGc.CMS_Perm_Gen.used': 23796992,
            prefix + '.memoryUsageBeforeGc.Code_Cache.max': 50331648,
            prefix + '.memoryUsageBeforeGc.Code_Cache.committed': 2686976,
            prefix + '.memoryUsageBeforeGc.Code_Cache.init': 2555904,
            prefix + '.memoryUsageBeforeGc.Code_Cache.used': 2600768,
            prefix + '.memoryUsageBeforeGc.Par_Survivor_Space.max': 3145728,
            prefix + '.memoryUsageBeforeGc.Par_Survivor_Space.committed': 3145728,
            prefix + '.memoryUsageBeforeGc.Par_Survivor_Space.init': 3145728,
            prefix + '.memoryUsageBeforeGc.Par_Survivor_Space.used': 414088
        }

    def get_metrics_rewrite_test(self):
        prefix = 'java.lang.name_ParNew.type_GarbageCollector.LastGcInfo'
        return {
            prefix + '.startTime': 14259063,
            prefix + '.id': 219,
            prefix + '.duration': 2,
            prefix + '.memUsedBeforeGc.Par_Eden_Space.max': 25165824,
            prefix + '.memUsedBeforeGc.Par_Eden_Space.committed': 25165824,
            prefix + '.memUsedBeforeGc.Par_Eden_Space.used': 25165824,
            prefix + '.memUsedBeforeGc.CMS_Old_Gen.max': 73400320,
            prefix + '.memUsedBeforeGc.CMS_Old_Gen.committed': 73400320,
            prefix + '.memUsedBeforeGc.CMS_Old_Gen.used': 5146840,
            prefix + '.memUsedBeforeGc.CMS_Perm_Gen.max': 85983232,
            prefix + '.memUsedBeforeGc.CMS_Perm_Gen.committed': 23920640,
            prefix + '.memUsedBeforeGc.CMS_Perm_Gen.used': 23796992,
            prefix + '.memUsedBeforeGc.Code_Cache.max': 50331648,
            prefix + '.memUsedBeforeGc.Code_Cache.committed': 2686976,
            prefix + '.memUsedBeforeGc.Code_Cache.used': 2600768,
            prefix + '.memUsedBeforeGc.Par_Survivor_Space.max': 3145728,
            prefix + '.memUsedBeforeGc.Par_Survivor_Space.committed': 3145728,
            prefix + '.memUsedBeforeGc.Par_Survivor_Space.used': 414088
        }


################################################################################
if __name__ == "__main__":
    unittest.main()
