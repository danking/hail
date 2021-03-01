PROJECT := hail-vdc
DOCKER_ROOT_IMAGE := gcr.io/$(PROJECT)/ubuntu:18.04
DOMAIN := hail.is
INTERNAL_IP := 10.128.0.2
IP := 35.224.188.20
KUBERNETES_SERVER_URL := https://34.123.185.11
REGION := us-central1
ZONE := us-central1-a
ifeq ($(NAMESPACE),default)
SCOPE = deploy
DEPLOY = true
else
SCOPE = dev
DEPLOY = false
endif
