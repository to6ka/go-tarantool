SHELL := /bin/bash
COVERAGE_FILE := coverage.out

.PHONY: clean
clean:
	( cd ./queue; rm -rf .rocks )
	rm -f $(COVERAGE_FILE)

.PHONY: deps
deps: clean
	( cd ./queue; tarantoolctl rocks install queue 1.1.0 )

.PHONY: test
test:
	go clean -testcache
	go test ./... -v -p 1	

.PHONY: coverage
coverage:
	go clean -testcache
	go get golang.org/x/tools/cmd/cover
	go test ./... -v -p 1 -covermode=count -coverprofile=$(COVERAGE_FILE)

.PHONY: coveralls
coveralls: coverage
	go get github.com/mattn/goveralls
	goveralls -coverprofile=$(COVERAGE_FILE) -service=github

.PHONY: bench
bench:
	go clean -testcache
	go test -bench=. -benchmem -benchtime=1s

.PHONY: ab-perf
ab-perf:
	export TNT_CUR_BRANCH=$(git branch --show-current)
	go clean -testcache
	go test -bench=. -benchmem -benchtime=1s > b.txt
	git stash && git fetch && git checkout master
	go clean -testcache
	go test -bench=. -benchmem -benchtime=1s > a.txt
	go get golang.org/x/tools/cmd/benchcmp
	benchcmp a.txt b.txt
	echo ${TNT_CUR_BRANCH}
	git checkout ${TNT_CUR_BRANCH}
	git stash pop
	unset TNT_CUR_BRANCH
