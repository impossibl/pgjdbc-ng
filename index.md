---
layout: default
---
## Features Included

* JDBC 4.1 target (with goal of complete conformance)
* UDT support through standard SQLData, SQLInput & SQLOutput
* Code Generator for UDTs ([https://github.com/impossibl/pgjdbc-ng-udt](https://github.com/impossibl/pgjdbc-ng-udt))
* Support for JDBC custom type mappings
* Pluggable custom type serialization
* Compact binary format with text format fallback
* Database, ResultSet and Parameter meta data
* Transactions / Savepoints
* Blobs
* Updatable ResultSets
* Callable Statements
* Asynchronous Notifications
* SSL Authentication and Encryption
* DataSource / XADataSource

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

The driver and data sources supports the following configuration
properties (name, type and default value).

	host      (String) localhost

The 'host' parameter specifies the host name of the database server. Data sources only.

	port         (int)     5432

The 'port' parameter specifies the port number of the database server. Data sources only.

	database     (String)

The 'database' parameter specifies the name of the database on the database server. Data sources only.

	user   (String)

The 'user' parameter specifies the user name.

	password     (String)

The 'password' parameter specifies the password.

	housekeeper      (boolean) true

The 'housekeeper' parameter specifies if the JDBC driver should keep track connections, statements and result sets
and automatically close them if they can't no longer be reached.

	parsedSqlCacheSize   (int)   250

The 'parsedSqlCacheSize' parameter specifies how big the cache size for parsed SQL statements should be.

	preparedStatementCacheSize (int)     50

The 'preparedStatementCacheSize' parameter specifies how big the cache size for PreparedStatement instaces should be.

	applicationName          (String)  pgjdbc app

The 'applicationName' parameter specifies the application name associated with the connection on the database server.

	clientEncoding          (String)  UTF8

The 'clientEncoding' parameter specifies the client encoding for the database server.

	networkTimeout         (int)     0

The 'networkTimeout' parameter specifies the default timeout in milliseconds for the connections.
A value of 0 indicates that there isnt a timeout for database operations.

	strictMode          (boolean)  false

The 'strictMode' parameter specifies if the JDBC driver should follow behavior assumed by certain frameworks, and test suites.
See http://github.com/impossibl/pgjdbc-ng/wiki/StrictMode for additional details.

	defaultFetchSize         (int)  0

The 'defaultFetchSize' parameter specifies the default fetch size for statements.

	receiveBufferSize         (int)     -1

The 'receiveBufferSize' parameter specifies the size of the receive buffer for the connection.

	sendBufferSize         (int)     -1

The 'sendBufferSize' parameter specifies the size of the send buffer for the connection.

	sslMode          (String)

The 'sslMode' parameter specifies which SSL mode that should be used to connect to the database server.
Valid values include 'prefer', 'require', 'verify-ca' and 'verify-full'.

	sslPassword          (String)

The 'sslPassword' parameter specifies the SSL password.

	sslCertificateFile          (String)

The 'sslCertificateFile' parameter specifies the SSL certificate file as a path.

	sslKeyFile          (String)

The 'sslKeyFile' parameter specifies the SSL key file as a path.

	sslRootCertificateFile          (String)

The 'sslRootCertificateFile' parameter specifies the SSL root certificate file as a path.

## License

pgjdbc-ng is released under the 3 clause BSD license.

## Releases

{% for post in site.categories.releases %}
* [ {{ post.title }} ]( {{ site.baseurl }}{{ post.url }} )  ( {{ post.date | date: "%b %d, %Y" }} )
{% endfor %}

## Requirements

* Java 7
* PostgreSQL 9.2

## Sponsors

YourKit is kindly supporting open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of innovative and intelligent tools for profiling
Java and .NET applications. Take a look at YourKit's leading software products:
[YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp) and [YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp)
