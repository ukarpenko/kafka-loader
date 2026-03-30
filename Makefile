APP_NAME := kafka-loader
IMAGE_NAME := kafka-loader-builder
CONTAINER_NAME := kafka-loader-artifact

WHEELHOUSE := wheelhouse
DIST := dist
DOCKERFILE := docker/builder.Dockerfile

PYTHON := python3

RUNTIME_PLATFORM := manylinux2014_x86_64
RUNTIME_PYTHON_VERSION := 311
RUNTIME_ABI := cp311
RUNTIME_IMPL := cp

CONFLUENT_WHEEL := $(WHEELHOUSE)/confluent_kafka-2.4.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl

.PHONY: help wheelhouse wheelhouse-runtime wheelhouse-build build-image extract-binary build-binary clean clean-wheelhouse

help:
	@echo "Targets:"
	@echo "  make wheelhouse        - download all wheels into wheelhouse/"
	@echo "  make build-image       - build Docker builder image"
	@echo "  make extract-binary    - extract compiled binary into dist/"
	@echo "  make build-binary      - build image and extract binary"
	@echo "  make clean             - remove build artifacts"
	@echo "  make clean-wheelhouse  - remove downloaded wheels"

$(WHEELHOUSE):
	mkdir -p $(WHEELHOUSE)

$(DIST):
	mkdir -p $(DIST)

wheelhouse: wheelhouse-runtime wheelhouse-build

wheelhouse-runtime: | $(WHEELHOUSE)
	$(PYTHON) -m pip download \
		--dest $(WHEELHOUSE) \
		--only-binary=:all: \
		--platform $(RUNTIME_PLATFORM) \
		--implementation $(RUNTIME_IMPL) \
		--python-version $(RUNTIME_PYTHON_VERSION) \
		--abi $(RUNTIME_ABI) \
		-r requirements-runtime.txt

wheelhouse-build: | $(WHEELHOUSE)
	$(PYTHON) -m pip download \
		--dest $(WHEELHOUSE) \
		--only-binary=:all: \
		--implementation $(RUNTIME_IMPL) \
		--python-version $(RUNTIME_PYTHON_VERSION) \
		-r requirements-build.txt

build-image:
	docker build -f $(DOCKERFILE) -t $(IMAGE_NAME) .

extract-binary: | $(DIST)
	-docker rm -f $(CONTAINER_NAME)
	docker create --name $(CONTAINER_NAME) $(IMAGE_NAME) /kafka-loader
	docker cp $(CONTAINER_NAME):/kafka-loader $(DIST)/$(APP_NAME)
	docker rm $(CONTAINER_NAME)

build-binary: build-image extract-binary

clean:
	rm -rf dist *.spec

clean-wheelhouse:
	rm -rf $(WHEELHOUSE)
