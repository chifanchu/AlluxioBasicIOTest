#!/bin/bash
cd /AlluxioBasicIOTest
mvn compile
mvn exec:exec -Dexec.executable="java" -Dexec.args="-classpath %classpath:../alluxio/core/client/target/alluxio-core-client-1.1.0-SNAPSHOT.jar:../alluxio/assembly/target/alluxio-assemblies-1.1.0-SNAPSHOT-jar-with-dependencies.jar -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8080 alluxioOperation.AlluxioBasicIO $1 $2"

