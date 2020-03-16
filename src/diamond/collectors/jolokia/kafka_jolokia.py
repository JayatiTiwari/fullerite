# -*- coding: utf-8 -*-

"""
Collect Kafka metrics using jolokia agent

### Example Configuration

```
    host = localhost
    port = 8778
```
"""

from jolokia import JolokiaCollector, MBean
import re
import sys

class KafkaJolokiaCollector(JolokiaCollector):
    TOTAL_TOPICS = re.compile('kafka\.server:name=.*PerSec,type=BrokerTopicMetrics')
    TIMER_METRICS = [
        "StdDev",
        "75thPercentile",
        "Mean",
        "98thPercentile",
        "99thPercentile",
        "95thPercentile",
        "Max",
        "Count",
        "50thPercentile",
        "Min",
        "999thPercentile",
    ]

    def collect_bean(self, prefix, obj):
        if isinstance(obj, dict) and "Count" in obj:
            if "Mean" in obj:
                for metric_key in self.TIMER_METRICS:
                    metric_value = obj.get(metric_key)
                    if metric_value:
                        self.parse_dimension_bean(prefix, metric_key.lower(), metric_value)
            else:
                counter_val = obj["Count"]
                self.parse_dimension_bean(prefix, "count", counter_val)
        else:
            for k, v in obj.iteritems():
                if type(v) in [int, float, long]:
                    self.parse_dimension_bean(prefix, k, v)
                elif isinstance(v, dict):
                    self.collect_bean("%s.%s" % (prefix, k), v)
                elif isinstance(v, list):
                    self.interpret_bean_with_list("%s.%s" % (prefix, k), v)

    def patch_dimensions(self, bean, dims):
        metric_name = dims.pop("name", None)
        metric_type = dims.pop("type", None)
        # If the prefix matches the TOTAL_TOPICS regular expression it means
        # that, metric has no topic associated with it and is really for all topics on that broker
        if re.match(self.TOTAL_TOPICS, bean.prefix):
            dims["topic"] = "_TOTAL_"
        # Handle multiple hosts mode
        if self.config["multiple_hosts_mode"] and self.current_host_identifier:
            dims["kafka_cluster"] = re.sub('^kafka-k8s', '', self.current_host_identifier)
        return metric_name, metric_type, dims

    # override to only return the hosts whose identifiers begin with kafka-k8s
    def patch_host_list(self, hosts):
        res = {}
        for service_name, value in hosts.iteritems():
            matched = re.match('^(kafka-k8s[\\w_-]+)', service_name)
            if matched:
                kafka_cluster = matched.group(1)
                res[kafka_cluster] = {
                    'host': value.get('host'),
                    'port': self.config['port']
                }
        return res

    def patch_metric_name(self, bean, metric_name_list):
        if self.config.get('prefix', None):
            metric_name_list = [self.config['prefix']] + metric_name_list

        metric_name_list.append(bean.bean_key.lower())
        return metric_name_list
