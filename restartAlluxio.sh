cd ../alluxio
./bin/alluxio-stop.sh all
./bin/alluxio format
./bin/alluxio-start.sh all SudoMount
