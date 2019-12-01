# This makefile has been created to help developers perform common actions.
# Most actions assume it is operating in a virtual environment where the
# python command links to the appropriate virtual environment Python.

MAKEFLAGS += --no-print-directory

# Do not remove this block. It is used by the 'help' rule when
# constructing the help output.
# help:
# help: Gestalt Makefile options:
# help:

# help: help                      - [default rule] display help information
.PHONY: help
help:
	@grep "^# help\:" Makefile | grep -v grep | sed 's/\# help\: //' | sed 's/\# help\://'


# help: test                      - run tests
.PHONY: test
test: generate certs
	@python -m unittest discover -s tests


# help: test-verbose              - run tests verbosely
.PHONY: test-verbose
test-verbose: generate certs
	@python -m unittest discover -s tests -v


# help: style                     - apply code formatter
.PHONY: style
style:
	@# Avoid formatting automatically generated code by excluding it
	@black src/gestalt tests setup.py examples docs/source --exclude .*_pb2\.py


# help: check-style               - check code formatting
.PHONY: check-style
check-style:
	@# Avoid formatting automatically generated code by excluding it
	@black src/gestalt tests setup.py examples docs/source --check --exclude .*_pb2\.py


# help: check-types               - check type hint annotations
.PHONY: check-types
check-types:
	@cd src; mypy -p gestalt --ignore-missing-imports


# help: check-lint                - run static analysis checks
.PHONY: check-lint
check-lint:
	@pylint --rcfile=.pylintrc gestalt


# help: check-coverage            - check collect code coverage
.PHONY: check-coverage
check-coverage: generate certs
	@coverage erase
	@coverage run -m unittest discover -s tests -v
	@coverage html
	@coverage report


# help: check-docs                - quick check docs consistency
.PHONY: check-docs
check-docs:
	@cd docs; make dummy


# help: docs                      - generate project documentation
.PHONY: docs
docs:
	@cd docs; rm -rf source/api/gestalt*.rst source/api/modules.rst build/*
	@cd docs; make html


# help: serve-docs                - serve project html documentation
.PHONY: serve-docs
serve-docs:
	@cd docs/build; python -m http.server --bind 127.0.0.1


# help: dist                      - create a wheel distribution package
.PHONY: dist
dist:
	@rm -rf dist
	@python setup.py bdist_wheel


# help: dist-test                 - test a wheel distribution package
.PHONY: dist-test
dist-test: dist
	@cd dist && ../tests/test-dist.bash ./gestalt-*-py3-none-any.whl


# help: dist-upload               - upload a wheel distribution package
.PHONY: dist-upload
dist-upload:
	@twine upload dist/gestalt-*-py3-none-any.whl


# help: clean                     - clean all files using .gitignore rules
.PHONY: clean
clean:
	@git clean -X -f -d


# help: scrub                     - clean all files, even untracked files
.PHONY: scrub
scrub:
	git clean -x -f -d


# help: generate                  - generate protobuf code stubs if needed
generate: tests/position_pb2.py \
	examples/amq/topic/position_pb2.py \
	examples/datagram/mti/position_pb2.py \
	examples/stream/mti/position_pb2.py


# help: regenerate                - force regenerate protobuf code stubs
regenerate:
	@rm -f tests/position_pb2.py
	@rm -f examples/amq/topic/position_pb2.py
	@rm -f examples/datagram/mti/position_pb2.py
	@rm -f examples/stream/mti/position_pb2.py
	@make generate


# help: certs                     - generate certificates for unit tests
certs: tests/certs/ca.key


# Keep these lines at the end of the file to retain nice help
# output formatting.
# help:

tests/position_pb2.py:
	@python -m grpc_tools.protoc -I proto --python_out=tests proto/position.proto

examples/amq/topic/position_pb2.py:
	@python -m grpc_tools.protoc -I proto --python_out=examples/amq/topic proto/position.proto

examples/datagram/mti/position_pb2.py:
	@python -m grpc_tools.protoc -I proto --python_out=examples/datagram/mti proto/position.proto

examples/stream/mti/position_pb2.py:
	@python -m grpc_tools.protoc -I proto --python_out=examples/stream/mti proto/position.proto


tests/certs/ca.key:
	@cd tests/certs && ./generate-certs.sh
