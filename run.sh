#!/usr/bin/env zsh
mvn clean compile exec:java -Dexec.mainClass=com.suyons.Main "$@"