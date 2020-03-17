#!/usr/bin/python
# coding=utf-8
################################################################################

import json
from test import CollectorTestCase
from test import get_collector_config
from test import unittest

from dimension_reader import NoopDimensionReader, CompositeDimensionReader, KubernetesDimensionReader
from test_readers import TestDimensionReader
import dimension_reader
from mock import Mock
from mock import patch


################################################################################

class TestNoopDimensionReader(CollectorTestCase):
    def setUp(self):
        self.config = get_collector_config('JolokiaCollector', {})
        self.dimension_reader = NoopDimensionReader()

    def test_import(self):
        self.assertTrue(NoopDimensionReader)

    def test_should_return_empty_dimension(self):
        self.dimension_reader.configure(self.config)
        actual = self.dimension_reader.read(['10.1.2.2', '10.1.2.3'])
        self.assertEquals({}, actual)


class TestCompositeDimensionReader(CollectorTestCase):
    def setUp(self):
        self.dimension_reader = CompositeDimensionReader()

    def test_import(self):
        self.assertTrue(CompositeDimensionReader)

    def test_should_return_empty_when_no_readers_configured(self):
        self.dimension_reader.configure({})
        actual = self.dimension_reader.read(['10.1.2.2', '10.1.2.3'])
        self.assertEquals({}, actual)

    def test_should_merge_dimension_from_multiple_readers(self):
        dim1 = {'host1': {'k1': 'v1'}, 'host2': {'k2': 'v2'}}
        dim2 = {'host1': {'k3': 'v3'}, 'host4': {'k4': 'v4'}}
        self.dimension_reader.configure({})
        self.dimension_reader.readers = [TestDimensionReader(dim1), TestDimensionReader(dim2)]

        expected = {'host1': {'k1': 'v1', 'k3': 'v3'}, 'host2': {'k2': 'v2'}, 'host4': {'k4': 'v4'}}
        actual = self.dimension_reader.read(['host1', 'host2', 'host4'])
        self.assertEquals(expected, actual)

    def test_should_not_fail_when_spec_not_set(self):
        self.dimension_reader.configure({})
        actual = self.dimension_reader.read([])
        self.assertEquals({}, actual)

    @patch.object(CompositeDimensionReader, 'read')
    def test_should_not_fail_when_invalid_reader_configured(self, mock_read):
        config = {
            "kubernetes": {
                "paasta_cluster": {
                    "paasta.yelp.com/cluster": ".*"
                }
            },
            "invalid": {}
        }
        self.dimension_reader.configure(config)
        self.dimension_reader.read()


class TestKubernetesDimensionReader(CollectorTestCase):
    def setUp(self):
        self.dimension_reader = KubernetesDimensionReader()

    def test_import(self):
        self.assertTrue(KubernetesDimensionReader)

    def test_should_generate_dimensions(self):
        def se():
            return json.loads(self.getFixture('pods.json').getvalue()), None

        with patch("kubernetes.Kubelet.list_pods", Mock(side_effect=se)):
            config = {
                "paasta_service": {
                    "paasta.yelp.com/service": ".*"
                },
                "paasta_instance": {
                    "paasta.yelp.com/instance": ".*"
                },
                "paasta_cluster": {
                    "paasta.yelp.com/cluster": ".*"
                }
            }
            self.dimension_reader.configure(config)
            actual = self.dimension_reader.read(['172.23.0.42'])
            expected = {
                '172.23.0.42': {
                    'paasta_service': 'kafka-operator',
                    'paasta_instance': 'main'
                }
            }
            self.assertEquals(expected, actual)

    def test_should_return_empty_if_none_match(self):
        def se():
            return json.loads(self.getFixture('pods.json').getvalue()), None

        with patch("kubernetes.Kubelet.list_pods", Mock(side_effect=se)):
            config = {
                "paasta_service": {
                    "test.yelp.com/service": ".*"
                },
                "paasta_instance": {
                    "test.yelp.com/instance": ".*"
                },
                "paasta_cluster": {
                    "test.yelp.com/cluster": ".*"
                }
            }
            self.dimension_reader.configure(config)
            actual = self.dimension_reader.read(['172.23.0.42'])
            expected = {'172.23.0.42': {}}
            self.assertEquals(expected, actual)

    def test_should_not_generate_dimensions(self):
        def se():
            return json.loads(self.getFixture('pods.json').getvalue()), None

        with patch("kubernetes.Kubelet.list_pods", Mock(side_effect=se)):
            self.dimension_reader.configure({})
            actual = self.dimension_reader.read(['10.1.1.1', '10.1.1.1'])
            self.assertEquals({}, actual)

    def test_should_not_fail_when_kubelet_fails(self):
        def se():
            return None, IOError("some error")

        with patch("kubernetes.Kubelet.list_pods", Mock(side_effect=se)):
            config = {
                "paasta_cluster": {
                    "paasta.yelp.com/cluster": ".*"
                }
            }
            self.dimension_reader.configure(config)
            actual = self.dimension_reader.read(['10.1.1.1', '10.1.1.1'])
            self.assertEquals({}, actual)


class TestDimensionReaderMethods(CollectorTestCase):

    def test_should_return_registered_reader(self):
        actual = dimension_reader.get_reader('kubernetes')
        self.assertEquals(type(KubernetesDimensionReader()), type(actual))

    def test_should_return_default_reader_for_invalid_type(self):
        actual = dimension_reader.get_reader('random')
        self.assertEquals(dimension_reader.DEFAULT_DIMENSION_READER, actual)


################################################################################


if __name__ == "__main__":
    unittest.main()
