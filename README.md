# pgjdbc-ng

## Building
The driver is built with maven. Simply build with:

	mvn clean package

This will produce, in the target directory, two JAR files. One with dependencies
packaged inside and another without.

## Testing

The unit tests need a PostgreSQL database to execute against. The tests assume theses defaults:

	SERVER: 	localhost
	PORT: 	        5432
	DATABASE:       test
	USERNAME:	pgjdbc
	PASSWORD:	test

The following system properties are supported in order to customize the setup

	pgjdbc.test.server
	pgjdbc.test.port
	pgjdbc.test.db
	pgjdbc.test.user
	pgjdbc.test.password

If you'd like to build the driver without running the unit tests use the command:

	mvn clean package -DskipTests
