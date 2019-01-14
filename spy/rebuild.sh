#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

(cd "$DIR/../spy-gen" ; mvn clean install )
mvn clean package
