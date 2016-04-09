#!/bin/bash
rm tmp*
rm file*
rm ../alluxioMount/tmp*
rm ../alluxioMount/file*
./copyDir.sh
cd ../alluxio
./bin/alluxio format
./bin/alluxio-start.sh all SudoMount