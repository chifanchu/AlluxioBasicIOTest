#!/bin/bash
rm /tmp/tmp*
rm /tmp/file*
./copyDir.sh
cd ../alluxio
./bin/alluxio format
./bin/alluxio-start.sh all SudoMount
