"""
dcos_history
"""

import logging
import re

from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)

class DcosHistory(base.JSONStatsPlugin):

    GUID = 'com.meetme.newrelic_dcos_history'

    def parse(self, data):
        assert data and data != "", "None response data"
        cluster_name = data['cluster']
        cluster_hostname = data['hostname']
        slaves = data['slaves']
        frameworks = data['frameworks']

        dcos_metrics = {}
        framework_metrics = {}
        slaves_metrics = {}

        dcos_metrics['tasks'] = {}
        dcos_metrics['tasks']['ERROR'] = 0
        dcos_metrics['tasks']['FAILED'] = 0
        dcos_metrics['tasks']['KILLED'] = 0
        dcos_metrics['tasks']['FINISHED'] = 0
        dcos_metrics['tasks']['LOST'] = 0
        dcos_metrics['tasks']['RUNNING'] = 0
        dcos_metrics['tasks']['STAGING'] = 0
        dcos_metrics['tasks']['STARTING'] = 0
        dcos_metrics['offered_resources'] = {}
        dcos_metrics['offered_resources']['cpus'] = 0
        dcos_metrics['offered_resources']['mem'] = 0
        dcos_metrics['offered_resources']['disk'] = 0
        dcos_metrics['resources'] = {}
        dcos_metrics['resources']['cpus'] = 0
        dcos_metrics['resources']['mem'] = 0
        dcos_metrics['resources']['disk'] = 0
        dcos_metrics['used_resources'] = {}
        dcos_metrics['used_resources']['cpus'] = 0
        dcos_metrics['used_resources']['mem'] = 0
        dcos_metrics['used_resources']['disk'] = 0

        for fm in frameworks:
            name = fm['name']
            framework_metrics[name] = {}
            framework_metrics[name]['tasks'] = {}
            framework_metrics[name]['offered_resources'] = {}
            framework_metrics[name]['resources'] = {}
            framework_metrics[name]['used_resources'] = {}

            framework_metrics[name]['tasks']['ERROR'] = fm['TASK_ERROR']
            framework_metrics[name]['tasks']['FAILED'] = fm['TASK_FAILED']
            framework_metrics[name]['tasks']['KILLED'] = fm['TASK_KILLED']
            framework_metrics[name]['tasks']['FINISHED'] = fm['TASK_FINISHED']
            framework_metrics[name]['tasks']['LOST'] = fm['TASK_LOST']
            framework_metrics[name]['tasks']['RUNNING'] = fm['TASK_RUNNING']
            framework_metrics[name]['tasks']['STAGING'] = fm['TASK_STAGING']
            framework_metrics[name]['tasks']['STARTING'] = fm['TASK_STARTING']
            framework_metrics[name]['offered_resources']['cpus'] = fm['offered_resources']['cpus']
            framework_metrics[name]['offered_resources']['mem'] = fm['offered_resources']['mem'] * 1024 * 1024
            framework_metrics[name]['offered_resources']['disk'] = fm['offered_resources']['disk'] * 1024 * 1024
            framework_metrics[name]['used_resources']['cpus'] = fm['used_resources']['cpus']
            framework_metrics[name]['used_resources']['mem'] = fm['used_resources']['mem'] * 1024 * 1024
            framework_metrics[name]['used_resources']['disk'] = fm['used_resources']['disk'] * 1024 * 1024

            dcos_metrics['tasks']['ERROR'] += fm['TASK_ERROR']
            dcos_metrics['tasks']['FAILED'] += fm['TASK_FAILED']
            dcos_metrics['tasks']['KILLED'] += fm['TASK_KILLED']
            dcos_metrics['tasks']['FINISHED'] += fm['TASK_FINISHED']
            dcos_metrics['tasks']['LOST'] += fm['TASK_LOST']
            dcos_metrics['tasks']['RUNNING'] += fm['TASK_RUNNING']
            dcos_metrics['tasks']['STAGING'] += fm['TASK_STAGING']
            dcos_metrics['tasks']['STARTING'] += fm['TASK_STARTING']
            dcos_metrics['offered_resources']['cpus'] += fm['offered_resources']['cpus']
            dcos_metrics['offered_resources']['mem'] += fm['offered_resources']['mem'] * 1024 * 1024
            dcos_metrics['offered_resources']['disk'] += fm['offered_resources']['disk'] * 1024 * 1024
            dcos_metrics['used_resources']['cpus'] += fm['used_resources']['cpus']
            dcos_metrics['used_resources']['mem'] += fm['used_resources']['mem'] * 1024 * 1024
            dcos_metrics['used_resources']['disk'] += fm['used_resources']['disk'] * 1024 * 1024

        for slave in slaves:
            name = slave['hostname']
            slaves_metrics[name] = {}
            slaves_metrics[name]['tasks'] = {}
            slaves_metrics[name]['offered_resources'] = {}
            slaves_metrics[name]['resources'] = {}
            slaves_metrics[name]['used_resources'] = {}

            slaves_metrics[name]['tasks']['ERROR'] = slave['TASK_ERROR']
            slaves_metrics[name]['tasks']['FAILED'] = slave['TASK_FAILED']
            slaves_metrics[name]['tasks']['KILLED'] = slave['TASK_KILLED']
            slaves_metrics[name]['tasks']['FINISHED'] = slave['TASK_FINISHED']
            slaves_metrics[name]['tasks']['LOST'] = slave['TASK_LOST']
            slaves_metrics[name]['tasks']['RUNNING'] = slave['TASK_RUNNING']
            slaves_metrics[name]['tasks']['STAGING'] = slave['TASK_STAGING']
            slaves_metrics[name]['tasks']['STARTING'] = slave['TASK_STARTING']
            slaves_metrics[name]['offered_resources']['cpus'] = slave['offered_resources']['cpus']
            slaves_metrics[name]['offered_resources']['mem'] = slave['offered_resources']['mem'] * 1024 * 1024
            slaves_metrics[name]['offered_resources']['disk'] = slave['offered_resources']['disk'] * 1024 * 1024
            slaves_metrics[name]['used_resources']['cpus'] = slave['used_resources']['cpus']
            slaves_metrics[name]['used_resources']['mem'] = slave['used_resources']['mem'] * 1024 * 1024
            slaves_metrics[name]['used_resources']['disk'] = slave['used_resources']['disk'] * 1024 * 1024
            slaves_metrics[name]['resources']['cpus'] = slave['resources']['cpus']
            slaves_metrics[name]['resources']['mem'] = slave['resources']['mem'] * 1024 * 1024
            slaves_metrics[name]['resources']['disk'] = slave['resources']['disk'] * 1024 * 1024
        return (dcos_metrics, framework_metrics, slaves_metrics)

    def add_datapoints(self, data):
        if data:
            dcos_metrics, framework_metrics, slaves_metrics = self.parse(data)
            for metrics_group in dcos_metrics.keys():
                for metric_item in dcos_metrics[metrics_group].keys():
                    value = dcos_metrics[metrics_group][metric_item]
                    metric_name = "history/cluster/%s/%s" % (metrics_group, metric_item)
                    if metrics_group == 'tasks':
                        unit = "tasks"
                    elif metric_item == 'cpus':
                        unit = "counts"
                    else:
                        unit = "bytes"
                    self.add_gauge_value(metric_name, unit, value)

            for fm in framework_metrics.keys():
                for metrics_group in framework_metrics[fm].keys():
                    for metric_item in framework_metrics[fm][metrics_group].keys():
                        value = framework_metrics[fm][metrics_group][metric_item]
                        metric_name = "history/framework/%s/%s/%s" % (fm, metrics_group, metric_item)
                        if metrics_group == 'tasks':
                            unit = "tasks"
                        elif metric_item == 'cpus':
                            unit = "counts"
                        else:
                            unit = "bytes"
                        self.add_gauge_value(metric_name, unit, value)

            for slave in slaves_metrics.keys():
                for metrics_group in slaves_metrics[slave].keys():
                    for metric_item in slaves_metrics[slave][metrics_group].keys():
                        value = slaves_metrics[slave][metrics_group][metric_item]
                        metric_name = "history/slave/%s/%s/%s" % (slave, metrics_group, metric_item)
                        if metrics_group == 'tasks':
                            unit = "tasks"
                        elif metric_item == 'cpus':
                            unit = "counts"
                        else:
                            unit = "bytes"
                        self.add_gauge_value(metric_name, unit, value)
        else:
            LOGGER.debug('Stats output: %r', data)
