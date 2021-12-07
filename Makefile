#
# Simplifies buidling and pushing to dockerhub
#

TAG = nbisweden/k8s-software-router

all: build

build:
	docker build -t ${TAG} .

push:
	docker push ${TAG}
