# coding=utf-8
import httplib

import diamond.collector

# envoy server metrics
METRICS_TO_METRIC_TYPE = {
    'server.concurrency': 'GAUGE',
    'server.days_until_first_cert_expiring': 'GAUGE',
    'server.debug_assertion_failures': 'COUNTER',
    'server.hot_restart_epoch': 'GAUGE',
    'server.live': 'GAUGE',
    'server.memory_allocated': 'GAUGE',
    'server.memory_heap_size': 'GAUGE',
    'server.parent_connections': 'GAUGE',
    'server.state': 'GAUGE',
    'server.total_connections': 'GAUGE',
    'server.uptime': 'GAUGE',
    'server.version': 'GAUGE',
    'server.watchdog_mega_miss': 'COUNTER',
    'server.watchdog_miss': 'COUNTER',
}


# the admin endpoint, listening on all hosts
ADMIN_IP = 'localhost'
ADMIN_PORT = 1338
ENDPOINT = '/stats?filter=^server.'
METHOD = 'GET'

class EnvoyServerStatsCollector(diamond.collector.Collector):
    """Collects envoy server metrics"""

    def collect(self):
        """Collects all server envoy metrics running on localhost"""
        base_metric_name = 'envoy.'

        connection = httplib.HTTPConnection(ADMIN_IP, ADMIN_PORT)
        connection.request(METHOD, ENDPOINT)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        for stat in data.split('\n')[:-1]:
            name, value = stat.split(': ')
            metric_type = METRICS_TO_METRIC_TYPE.get(name, None)
            if metric_type:
                self.publish(base_metric_name + name, int(value), metric_type=metric_type)
