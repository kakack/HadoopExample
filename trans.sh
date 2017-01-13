#!/bin/bash
mvn compile
mvn package
scp ./target/HadoopExample-1.0-SNAPSHOT.jar ubuntu@namenode:/hadoop/hadoop
