#!/bin/bash

_DIR=`dirname $0`
_DIR=`cd $_DIR; pwd`

node ${_DIR}/cleanup-v2.js -m mongodb://127.0.0.1:27017 --acs-url http://gcs.x.com:8081 $@

