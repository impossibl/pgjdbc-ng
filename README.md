[![](https://img.shields.io/travis/impossibl/pgjdbc-ng/develop.svg?style=flat)](https://travis-ci.org/impossibl/pgjdbc-ng/branches)
[![](https://img.shields.io/github/release/impossibl/pgjdbc-ng.svg?style=flat)](https://github.com/impossibl/pgjdbc-ng/releases/latest)

### General

A great place to start is the [Website](https://impossibl.github.io/pgjdbc-ng).

For comprehensive documentation see the [User Guide](https://impossibl.github.io/pgjdbc-ng/docs/current/user-guide)

For quick reference, here are some useful details

#### Dependencies

[![](https://img.shields.io/maven-central/v/com.impossibl.pgjdbc-ng/pgjdbc-ng.svg)](https://search.maven.org/search?q=g:com.impossibl.pgjdbc-ng%20AND%20a:pgjdbc-ng&core=gav)

##### Gradle

```groovy
compile "com.impossibl.pgjdbc-ng:pgjdbc-ng:LATEST"
```
    
##### Maven

```xml
<dependency>
  <groupId>com.impossibl.pgjdb-ng</groupId>
  <artifactId>pgjdb-ng</artifactId>
  <version>LATEST</version>
</dependency>
```
    

#### JDBC URL

The driver accepts basic URLs in the following format

	jdbc:pgsql://localhost:5432/db
	

See the [User Guide](https://impossibl.github.io/pgjdbc-ng/docs/current/user-guide#connection-urls) 
for complete details on the accepted URL syntax.

#### Data sources

The javax.sql.DataSource class is

	com.impossibl.postgres.jdbc.PGDataSource

, the javax.sql.ConnectionPoolDataSource class is

	com.impossibl.postgres.jdbc.PGConnectionPoolDataSource

and the XADataSource class is

	com.impossibl.postgres.jdbc.xa.PGXADataSource

#### License

pgjdbc-ng is released under the 3 clause BSD license.

#### Building
The driver is built with Gradle. To build & test, execute:

	./gradlew clean build.

This will produce, in the `driver/build/libs` directory, two JAR files. One with dependencies
packaged inside (`pgjdbg-nc-<VERSION>-all`) and another without (`pgjdbc-ng-VERSION`).

*NOTE:* Building requires a working install of [Docker](https://docs.docker.com/docker) and 
[Docker Compose](https://docs.docker.com/compose) as the unit tests are executed against a
PostgreSQL instance in a private test container that is automatically started. If you wish to
execute the tests against a specific instance of PostgreSQL outside of Docker see [Testing](#Testing).

Alternatively, to build the driver without testing you can execute:

    ./gradlew clean assemble  

#### Testing

The unit tests need a PostgreSQL database to execute against. The build will start a [Docker](https://docker.com)
container and setup the test environment to execute against that container automatically.

If you don't have Docker installed or, wish to execute the tests against a specific instance of PostgreSQL, you
can execute the `build` or `test` tasks with the `noDocker` property set to true. For example:

    ./gradle -PnoDocker=true test
    
For this to work the unit tests need to locate your selected PostgreSQL instance. The unit tests attempt a
connection assuming theses defaults:

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
