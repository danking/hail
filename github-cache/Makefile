include ../config.mk

GITHUB_CACHE_LATEST = gcr.io/$(PROJECT)/github-cache:latest
GITHUB_CACHE_IMAGE = gcr.io/$(PROJECT)/github-cache:$(shell docker images -q --no-trunc github-cache | sed -e 's,[^:]*:,,')

.PHONY: build
build:
	$(MAKE) -C ../docker hail-ubuntu
	-docker pull $(GITHUB_CACHE_LATEST)
	python3 ../ci/jinja2_render.py '{"hail_ubuntu_image":{"image":"hail-ubuntu"}}' Dockerfile Dockerfile.out
	docker build -t github-cache -f Dockerfile.out --cache-from github-cache,$(GITHUB_CACHE_LATEST),hail-ubuntu .

.PHONY: push
push: build
	docker tag github-cache $(GITHUB_CACHE_LATEST)
	docker push $(GITHUB_CACHE_LATEST)
	docker tag github-cache $(GITHUB_CACHE_IMAGE)
	docker push $(GITHUB_CACHE_IMAGE)

.PHONY: deploy
deploy: push
	python3 ../ci/jinja2_render.py '{"code":{"sha":"$(shell git rev-parse --short=12 HEAD)"},"deploy":true,"github_cache_image":{"image":"$(GITHUB_CACHE_IMAGE)"},"default_ns":{"name":"default"},"global":{"domain":"$(DOMAIN)"}}' deployment.yaml deployment.yaml.out
	kubectl -n default apply -f deployment.yaml.out

.PHONY: clean
clean:
	rm -rf deployment.yaml
