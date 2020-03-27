# coding=utf-8
import requests

import diamond.collector


# envoy server metrics
METRICS_TO_COLLECT_TO_TYPE = [
    ('concurrency', 'GAUGE'),
    ('days_until_first_cert_expiring', 'GAUGE'),
    ('debug_assertion_failures', 'COUNTER'),
    ('hot_restart_epoch', 'GAUGE'),
    ('live', 'GAUGE'),
    ('memory_allocated', 'GAUGE'),
    ('memory_heap_size', 'GAUGE'),
    ('parent_connections', 'GAUGE'),
    ('state', 'GAUGE'),
    ('total_connections', 'GAUGE'),
    ('uptime', 'GAUGE'),
    ('version', 'GAUGE'),
    ('watchdog_mega_miss', 'COUNTER'),
    ('watchdog_miss', 'COUNTER'),
]

# admin endpoint, listening on all hosts
ADMIN_URL = "http://localhost:1338/stats"

class EnvoyServerStatsCollector(diamond.collector.Collector):
    """Collects envoy server metrics"""

    def collect(self):
        """Collects all server envoy metrics running on localhost"""
        base_metric_name = "envoy.server."

        for metric, collection_type in METRICS_TO_COLLECT_TO_TYPE:
            response = requests.get(
                ADMIN_URL,
                params={'filter': 'server.{}'.format(metric)},
            )

            # format of response is `matric: value`, thus we split on `: `
            _, value = response.text.split(': ')

            self.publish(base_metric_name + metric, int(value), metric_type=collection_type)
