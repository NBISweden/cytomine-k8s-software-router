#
# Simplifies buidling and pushing to dockerhub
#

TAG = norling/k8s-software-router

all: build

build:
	docker build -t ${TAG} .

push:
	docker push ${TAG}
