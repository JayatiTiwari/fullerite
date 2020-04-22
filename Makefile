FULLERITE      := fullerite
BEATIT         := beatit
VERSION        := 0.6.88
SRCDIR         := src
GLIDE          := glide
HANDLER_DIR    := $(SRCDIR)/fullerite/handler
PROTO_SFX      := $(HANDLER_DIR)/signalfx.proto
GEN_PROTO_SFX  := $(HANDLER_DIR)/signalfx.pb.go
EXTRA_VERSION  ?= 0
PKGS           := \
	$(FULLERITE) \
	$(FULLERITE)/$(BEATIT) \
	$(FULLERITE)/collector \
	$(FULLERITE)/config \
	$(FULLERITE)/handler \
	$(FULLERITE)/internalserver \
	$(FULLERITE)/metric \
	$(FULLERITE)/util \
	$(FULLERITE)/dropwizard

SOURCES        := $(foreach pkg, $(PKGS), $(wildcard $(SRCDIR)/$(pkg)/*.go))
SOURCES        := $(filter-out $(GEN_PROTO_SFX), $(SOURCES))
OS	       := $(shell /usr/bin/lsb_release -si 2> /dev/null)

space :=
space +=
comma := ,

# symlinks confuse go tools, let's not mess with it and use -L
GOPATH  := $(shell pwd -L)
export GOPATH

PATH := $(GOPATH)/bin:$(GOPATH)/go/bin:$(PATH)
export PATH

# Use yelp-internal pypi if building at Yelp.
ifeq ($(findstring .yelpcorp.com,$(shell hostname -f)), .yelpcorp.com)
    export PIP_INDEX_URL ?= https://pypi.yelpcorp.com/simple
else
    export PIP_INDEX_URL ?= https://pypi.python.org/simple
endif

all: clean fmt lint $(FULLERITE) $(BEATIT) test

.PHONY: clean
clean:
	@echo Cleaning $(FULLERITE)...
	@rm -f $(FULLERITE) bin/$(FULLERITE)
	@rm -f $(BEATIT) bin/$(BEATIT)
	@rm -rf pkg/*/$(FULLERITE)
	@rm -rf build fullerite*.deb fullerite*.rpm
	@-find . -name '*.py[co]' -delete
	@rm -rf .tox
# Let's keep the generated file in the repo for ease of development.
#	@rm -f $(GEN_PROTO_SFX)

deps:
	@echo Getting dependencies...
	@go get github.com/Masterminds/glide
	@cd src/github.com/Masterminds/glide && git checkout --quiet v0.12.3
	@go build -o bin/glide github.com/Masterminds/glide/
	@rm -rf $(GOPATH)/src/$(FULLERITE)/vendor/
	@cd $(GOPATH)/src/$(FULLERITE) && $(GLIDE) install
	@go build -o bin/golint fullerite/vendor/github.com/golang/lint/golint/
	@go build -o bin/gocyclo fullerite/vendor/github.com/fzipp/gocyclo/

$(FULLERITE): $(SOURCES) deps
	@echo Building $(FULLERITE)...
	@go build -ldflags '-X main.version=$(VERSION)' -o bin/$(FULLERITE) $@

$(BEATIT): $(BEATIT_SOURCES)
	@echo Building $(BEATIT)...
	@go build -ldflags '-X main.version=$(VERSION)' -o bin/$(BEATIT) fullerite/beatit

go:
	uname -a |grep -qE '^Linux.*x86_64' && curl -s https://dl.google.com/go/go1.13.linux-amd64.tar.gz | tar xz

test: tests
tests: deps diamond_test fullerite-tests

fullerite-tests:
	@echo Testing $(FULLERITE)
	@for pkg in $(PKGS); do \
		go test -race -cover $$pkg || exit 1;\
	done

qbt:
	@echo Fast testing $(FULLERITE)
	@for pkg in $(PKGS); do \
		go test -v -cover $(GO_TEST_ARGS) $$pkg || exit 1;\
	done

qct:
	@echo Fast testing fullerite collectors
	@for pkg in $(FULLERITE)/collector; do \
		go test -v -cover $$pkg $(GO_TEST_ARGS) || exit 1;\
	done

diamond_test:
	@tox

coverage_report: deps
	@echo Creating a coverage rport for $(FULLERITE)
	@$(foreach pkg, $(PKGS), go test -coverprofile=coverage.out -coverpkg=$(subst $(space),$(comma),$(PKGS)) $(pkg);)
	@go tool cover -html=coverage.out

fmt: deps $(SOURCES)
	@$(foreach pkg, $(PKGS), go fmt $(pkg);)

vet: deps $(SOURCES)
	@echo Vetting $(FULLERITE) sources...
	@$(foreach pkg, $(PKGS), go vet $(pkg);)

proto: protobuf
protobuf: deps $(PROTO_SFX)
	@echo Compiling protobuf
	@go get -u github.com/golang/protobuf/proto
	@go get -u github.com/golang/protobuf/protoc-gen-go
	@protoc --go_out=. $(PROTO_SFX)

lint: deps $(SOURCES)
	@echo Linting $(FULLERITE) sources...
	@$(foreach src, $(SOURCES), golint $(src);)

cyclo: deps $(SOURCES)
	@echo Checking code complexity...
	@gocyclo $(SOURCES)

pkg: package
package: clean $(FULLERITE) $(BEATIT)
	@echo Packaging for $(OS)
	@mkdir -p build/usr/bin build/usr/share/fullerite build/etc
	@cp bin/fullerite build/usr/bin/
	@cp bin/beatit build/usr/bin/
	@cp deb/bin/run-* build/usr/bin/
	@cp examples/config/fullerite.conf.example build/etc/
	@cp -r src/diamond build/usr/share/fullerite/diamond
ifeq ($(OS),Ubuntu)
	@fpm -s dir \
		-t deb \
		--name $(FULLERITE) \
		--version $(VERSION) \
		--description "metrics collector" \
		--depends python \
		--deb-user "fuller" \
		--deb-group "fuller" \
		--deb-default "deb/etc/fullerite" \
		--deb-upstart "deb/etc/init/fullerite" \
		--deb-upstart "deb/etc/init/fullerite_diamond_server" \
		--deb-systemd "deb/etc/systemd/fullerite" \
		--deb-systemd "deb/etc/systemd/fullerite_diamond_server" \
		--before-install "deb/before_install.sh" \
		--before-remove "deb/before_rm.sh" \
		--after-remove "deb/post_rm.sh" \
		--iteration "$(EXTRA_VERSION)" \
		-C build .
# CentOS 7 Only
else ifeq ($(OS),CentOS)
	@fpm -s dir \
		-t rpm \
		--name $(FULLERITE) \
		--version $(VERSION) \
		--description "metrics collector" \
		--depends python \
		--rpm-user "fuller" \
		--rpm-group "fuller" \
		--before-install "rpm/before_install.sh" \
		--before-remove "rpm/before_rm.sh" \
		--iteration "$(EXTRA_VERSION)" \
		-C build . \
		../rpm/fullerite.systemd=/etc/systemd/system/fullerite.service \
		../rpm/fullerite.sysconfig=/etc/sysconfig/fullerite
else ifeq ($(OS),AmazonAMI)
	@fpm -s dir \
		-t rpm \
		--name $(FULLERITE) \
		--version $(VERSION) \
		--description "metrics collector" \
		--depends python \
		--rpm-user "fuller" \
		--rpm-group "fuller" \
		--before-install "rpm/before_install.sh" \
		--before-remove "rpm/before_rm.sh" \
		--iteration "$(EXTRA_VERSION)" \
		-C build . \
		../deb/etc/init/fullerite=/etc/init/fullerite.conf \
		../deb/etc/init/fullerite_diamond_server=/etc/init/fullerite_diamond_server.conf
else
	@echo "OS not supported"
endif
