MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR       := $(dir $(MKFILE_PATH))

ifeq ("$(DOCKER_ORG)","")
$(warning WARNING: No docker user found, using graphscope)
DOCKER_ORG       		= graphscope
endif

.PHONY: submodule gsruntime gsvineyard graphscope push clean

ifeq ($(REGISTRY),)
    REGISTRY := registry.cn-hongkong.aliyuncs.com
endif

VERSION ?= latest
PROFILE ?= release

frontend-runtime:
	cd $(WORKING_DIR)/../.. && docker build -t ${REGISTRY}/$(DOCKER_ORG)/frontend-runtime:${VERSION} \
		-f $(WORKING_DIR)/runtime/frontend.Dockerfile .

executor-runtime:
	cd $(WORKING_DIR)/../.. && docker build -t ${REGISTRY}/$(DOCKER_ORG)/executor-runtime:${VERSION} \
		-f $(WORKING_DIR)/runtime/executor.Dockerfile .

graphscope-frontend:
	cd $(WORKING_DIR)/../.. && docker build --build-arg profile=$(PROFILE) --target frontend -t ${REGISTRY}/$(DOCKER_ORG)/graphscope-frontend:${VERSION} -f $(WORKING_DIR)/build.Dockerfile .

graphscope-executor:
	cd $(WORKING_DIR)/../.. && docker build --build-arg profile=$(PROFILE) --target executor -t ${REGISTRY}/$(DOCKER_ORG)/graphscope-executor:${VERSION} -f $(WORKING_DIR)/build.Dockerfile .

push:
	docker push ${REGISTRY}/$(DOCKER_ORG)/frontend-runtime:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/executor-runtime:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/graphscope-frontend:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/graphscope-executor:${VERSION}

clean:
	docker ps -qa | xargs $(XARGS_EMPTY_FLAG) docker rm -f
	docker images -f "dangling=true" -q | xargs $(XARGS_EMPTY_FLAG) docker rmi -f
