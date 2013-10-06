# pgjdbc-ng

## Building
The driver is built with maven. Simply build with:

	mvn clean package

This will produce, in the target directory, two JAR files. One with dependencies
packaged inside and another without.

## Testing

The unit tests need a PostgreSQL database to execute against. The tests assume theses defaults:

	DATABASE: test
	SERVER: 	localhost
	USERNAME:	postgres
	PASSWORD:	test

If you'd like to build the driver without running the unit tests use the command:

	mvn clean package -DskipTests
