# Hacking

## Running tests

You need to [install Tarantool](https://www.tarantool.io/en/download/) to run tests.
See [Installation](#installation) section for requirements.

To install test dependencies (like [tarantool/queue](https://github.com/tarantool/queue) module), run
```bash
make deps
```

To run tests for the main package and each subpackage, call
```bash
make test
```
Tests set up all required `tarantool` processes before run and clean up after.

If you want to run a specific package tests, call
```bash
make test-<SUBDIR>
```
For example, for running tests in `multi`, `uuid` and `main` packages, call
```bash
make test-multi test-uuid test-main
```
