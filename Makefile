# Go parameters
BINARY_NAME=main
DOCKER_IMAGE_NAME=arbichan
DOCKER_IMAGE_TAG:=latest

all: test build

build:
	$(GOBUILD) -o $(BINARY_NAME) -v

test:
	$(GOTEST) -v ./...

clean:
	go clean
	rm -f $(BINARY_NAME)

run: build
	./$(BINARY_NAME)

setup:
	go mod tidy

lint:
	go fmt ./...
	go vet ./...

docker/build:
	docker build --platform=linux/amd64 -t $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG) .

docker/run:
	docker run --rm --name $(DOCKER_IMAGE_NAME) $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)

.PHONY: $(shell grep -E '^([a-zA-Z_-]|\/)+:' $(MAKEFILE_LIST) | awk -F':' '{print $$2}' | sed 's/:.*//')
