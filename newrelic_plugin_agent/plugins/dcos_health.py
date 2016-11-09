"""
dcos_health
"""

import logging
import re

from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)

class DcosHealth(base.JSONStatsPlugin):

    GUID = 'com.meetme.newrelic_dcos'

    def parse(self, data):
        assert data and data != "", "None response data"
        units = data['Units']
        nodes = data['Nodes']

        # initialize dicts to aggregate metrics
        dcos_metrics = {
            'units': {
                'healthy': 0,
                'unhealthy': 0
            },
            'nodes': {
                'master': {
                    'healthy': 0,
                    'unhealthy': 0
                },
                'agent': {
                    'healthy': 0,
                    'unhealthy': 0
                }
            }
        }
        units_metrics = {}
        nodes_metrics = {}

        # get stats for each unit and its nodes
        for unitkey in units:
            unit = units[unitkey]
            name = unit['UnitName']
            units_metrics[name] = {}
            units_metrics[name]['health'] = (1 if unit['Health'] == 0 else 0)

            if unit['Health'] == 0:
                dcos_metrics['units']['healthy'] += 1
            else:
                dcos_metrics['units']['unhealthy'] += 1

            units_metrics[name]['node'] = {}
            for unode in unit['Nodes']:
                unname = unode['IP']
                units_metrics[name]['node'][unname] = {}
                units_metrics[name]['node'][unname]['health'] = (1 if unode['Health'] == 0 else 0)

        # get stats for each node and its units
        for nodekey in nodes:
            node = nodes[nodekey]
            name = node['IP']
            nodes_metrics[name] = {}
            nodes_metrics[name]['leader'] = (1 if node['Leader'] else 0)
            nodes_metrics[name]['health'] = (1 if node['Health'] == 0 else 0)

            if node['Health'] == 0:
                dcos_metrics['nodes'][node['Role']]['healthy'] += 1
            else:
                dcos_metrics['nodes'][node['Role']]['unhealthy'] += 1

            nodes_metrics[name]['unit'] = {}
            for nunit in node['Units']:
                nuname = nunit['UnitName']
                nodes_metrics[name]['unit'][nuname] = {}
                nodes_metrics[name]['unit'][nuname]['health'] = (1 if nunit['Health'] == 0 else 0)

        return (dcos_metrics, units_metrics, nodes_metrics)

    def add_datapoints(self, data):
        if data:
            dcos_metrics, units_metrics, nodes_metrics = self.parse(data)

            # summary metrics
            for item in dcos_metrics['units'].keys():
                metric_name = "health/cluster/units/%s" % item
                self.add_gauge_value(metric_name, 'counts', dcos_metrics['units'][item])

            for role in dcos_metrics['nodes'].keys():
                for item in dcos_metrics['nodes'][role].keys():
                    metric_name = "health/cluster/nodes/%s/%s" % (role, item)
                    self.add_gauge_value(metric_name, 'counts', dcos_metrics['nodes'][role][item])

            # unit metrics
            for unit in units_metrics.keys():
                metric_name = "health/unit/%s/health" % unit
                self.add_gauge_value(metric_name, 'counts', units_metrics[unit]['health'])

                for node in units_metrics[unit]['node'].keys():
                    for item in units_metrics[unit]['node'][node].keys():
                        metric_name = "health/unit/%s/node/%s/%s" % (unit, node, item)
                        self.add_gauge_value(metric_name, 'counts', units_metrics[unit]['node'][node][item])

            # node metrics
            for node in nodes_metrics.keys():
                for item in ['leader', 'health']:
                    metric_name = "health/node/%s/%s" % (node, item)
                    self.add_gauge_value(metric_name, 'counts', nodes_metrics[node][item])

                for unit in nodes_metrics[node]['unit'].keys():
                    for item in nodes_metrics[node]['unit'][unit].keys():
                        metric_name = "health/node/%s/unit/%s/%s" % (node, unit, item)
                        self.add_gauge_value(metric_name, 'counts', nodes_metrics[node]['unit'][unit][item])
        else:
            LOGGER.debug('Stats output: %r', data)
