Cytomine Kubernetes Software Router
===================================

This is a cytomine software router replacement intended to work in a kubernetes
environment.


# Development in minikube

To build your image into minikube, set your terminal to use the minikube docker
daemon using `eval $(minikube docker-env)`, then build your image as usual with
`docker build -t cytomine/k8s_software_router .`. Make sure to set
`imagePullPolicy` to `Never` in kuberenetes, or it will attempt to download the
image.
