#!/bin/bash

script_path=$(dirname "${BASH_SOURCE[0]}")

javac "$script_path"/Verify.java
java -cp "$script_path:$1" Verify "${2:-jdbc:pgsql://test:test@localhost:5432/test}"
