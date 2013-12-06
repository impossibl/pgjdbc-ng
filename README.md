# pgjdbc-ng

A new JDBC driver for PostgreSQL aimed at supporting the advanced features of JDBC and Postgres

[http://impossibl.github.io/pgjdbc-ng](http://impossibl.github.io/pgjdbc-ng "pgjdbc-ng homepage")

## Connection format

The connection format for the pgjdbc-ng driver is

	jdbc:pgsql://<server>[:<port>]/<database>

An example

	jdbc:pgsql://localhost:5432/test

## DataSource / XADataSource

The DataSource class is

	com.impossibl.postgres.jdbc.PGDataSource

and the XADataSource class is

	com.impossibl.postgres.jdbc.xa.PGXADataSource

Both implementations supports the following properties

	Host:		localhost
	Port:		5432
	Database:	<empty>
	User:		<empty>
	Password:	<empty>
	Housekeeper:	true

## License

pgjdbc-ng is released under the 3 clause BSD license.

## Building
The driver is built with maven. Simply build with:

	mvn clean package

This will produce, in the target directory, two JAR files. One with dependencies
packaged inside and another without.

## Testing

The unit tests need a PostgreSQL database to execute against. The tests assume theses defaults:

	SERVER:     localhost
	PORT:       5432
	DATABASE:   test
	USERNAME:   pgjdbc
	PASSWORD:   test

The following system properties are supported in order to customize the setup

	pgjdbc.test.server
	pgjdbc.test.port
	pgjdbc.test.db
	pgjdbc.test.user
	pgjdbc.test.password

If you'd like to build the driver without running the unit tests use the command:

	mvn clean package -DskipTests
