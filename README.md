# pgjdbc-ng   [![Build Status](https://travis-ci.org/impossibl/pgjdbc-ng.png)](https://travis-ci.org/impossibl/pgjdbc-ng)

A new JDBC driver for PostgreSQL aimed at supporting the advanced features of JDBC and Postgres

[http://impossibl.github.io/pgjdbc-ng](http://impossibl.github.io/pgjdbc-ng "pgjdbc-ng homepage")

## Connection format

The connection format for the pgjdbc-ng driver is

	jdbc:pgsql://<server>[:<port>]/<database>

An example

	jdbc:pgsql://localhost:5432/test

## Driver

The java.sql.Driver class is

	com.impossibl.postgres.jdbc.PGDriver

The driver will accept configuration parameters in the style of

	jdbc:pgsql://localhost:5432/test?applicationName=MyApp&networkTimeout=10000

## Data sources

The javax.sql.DataSource class is

	com.impossibl.postgres.jdbc.PGDataSource

, the javax.sql.ConnectionPoolDataSource class is

	com.impossibl.postgres.jdbc.PGConnectionPoolDataSource

and the XADataSource class is

	com.impossibl.postgres.jdbc.xa.PGXADataSource

## Configuration
 
### Settings

The `Driver` supports configuration via a large number of number of settings that can be specified as system 
properties or per connection via URL parameters and/or driver properties.

The `DataSource` supports the same configurability through system properties or via distinct methods on the data
source classes.

A detailed list of available settings is available [HERE](SETTINGS.md) 

### Host Addresses

#### PGDriver
The driver supports specifying multiple host addresses via the URL to provide fallback addresses while attempting to 
connect to a server. Each host will be tried in the order specified until a connection can be made.

##### Multi-host Address URL Format

    jdbc:pgsql://host1,host2:5434,127.0.0.1/db

This format allows providing multiple addresses separated with commas. Each address can be specified as an `IPv4`,
`IPv6`, or `DNS` address followed by an optional port. Any number of addresses can be specified.
  
> In the event of a successful connection and subsequent unexpected disconnection, no attempt is made to 
re-establish a connection regardless of the presence of fallback addresses. 

##### Unix Domain Sockets

When Netty native libraries are present the driver can connect to a PostgreSQL instance on the same machine via
its local socket. You specify this connection address using a special `unixsocket` property supplied via the URL.

    jdbc:pgsql:db?unixsocket=/tmp 

The property accepts a directory containing a PostgreSQL unix socket (as shown above), in which case it searches
the directory for a socket filename matching PostgresSQL's known format. Alternatively, you can specify a complete 
absolute path to the unix socket. Only a single `unixsocket` property can be specified.

> When combining the `unixsocket` property and internet addresses in a fallback configuration the unix socket 
address is always attempted first.

> Visit [Netty's wiki](https://netty.io/wiki/native-transports.html) for more information on how to acquire the correct native libraries for your
platform  

#### Data Sources

Each `DataSource` implementation currently allows specifying at most a single host address. No support for 
fallback addresses is currently implemented.

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
	USERNAME:   test
	PASSWORD:   test

The following system properties are supported in order to customize the setup

	pgjdbc.test.server
	pgjdbc.test.port
	pgjdbc.test.db
	pgjdbc.test.user
	pgjdbc.test.password

If you'd like to build the driver without running the unit tests use the command:

	mvn clean package -DskipTests
