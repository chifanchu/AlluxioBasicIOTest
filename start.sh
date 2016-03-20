#!/bin/bash

cd ../alluxio
./bin/alluxio format
./bin/alluxio-start.sh all SudoMount
