docker:
	mkdir -p /tmp/nrdcos/newrelic_plugin_agent/
	cp -p * /tmp/nrdcos
	cp -pr ../../newrelic_plugin_agent/ /tmp/nrdcos/
	docker build -t mikewillcook/newrelicdcos /tmp/nrdcos
	rm -rf /tmp/nrdcos

docker-push:
	docker push mikewillcook/newrelicdcos

docker-run:
	docker run --rm -it -e NEWRELIC_KEY=none mikewillcook/newrelicdcos
