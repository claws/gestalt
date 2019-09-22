#!/usr/bin/env bash
#
# This script creates a virtual environment and installs a distribution of
# this project into the environment and then executes the unit tests. This
# provides a crude check that the distribution is installable and that the
# package is minimally functional.
#

if [ -z "$1" ]; then
  echo "usage: $0 gestalt-YY.MM.MICRO-py3-none-any.whl"
  exit
fi

RELEASE_ARCHIVE="$1"

echo "Release archive: $RELEASE_ARCHIVE"

echo "Removing any old artefacts"
rm -rf test_venv

echo "Creating test virtual environment"
python -m venv test_venv

echo "Entering test virtual environment"
source test_venv/bin/activate

echo "Upgrading pip"
pip install pip --upgrade
pip install asynctest

echo "Installing $RELEASE_ARCHIVE"
pip install $RELEASE_ARCHIVE[amq,protobuf,msgpack,avro,brotli,snappy,yaml]

echo "Running tests"
cd ../tests
python -m unittest discover -s . -v

echo "Exiting test virtual environment"
deactivate
