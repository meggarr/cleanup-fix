#!/bin/bash

node ./cleanup.js -m mongodb://199.71.142.249:27017,199.71.143.62:27017,199.71.143.63:27017/?replicaSet=cmdctr --acs-url http://gcs.x.com:8081 $@

