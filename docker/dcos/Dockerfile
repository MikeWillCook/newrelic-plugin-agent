#
# Mesosphere DC/OS plugin docker image
#

FROM python:2.7-alpine
MAINTAINER mikewillcook@gmail.com

RUN pip install helper requests

# Setup the newrelic-plugin-agent and defaults
ADD newrelic_plugin_agent/ /opt/newrelic_plugin_agent/
ADD newrelic-plugin-agent.cfg /etc/newrelic/newrelic-plugin-agent.cfg

# Startup the plugin agent
ENV PYTHONPATH=/opt
CMD ["/opt/newrelic_plugin_agent/agent.py","-c","/etc/newrelic/newrelic-plugin-agent.cfg","-f"]
