# UDT Generator

Supporting SQL **U**ser **D**efined **T**ypes in JDBC 4.2 requires creating a POJO that implements the `SQLData`
interface. Although this is fairly straightforward it can be time consuming to keep Java objects up-to-date with a
changing SQL schema. This generator provides a simple way to generate UDT classes from an available schema.

The generator will connect to a provided database, lookup information about a provided set of UDTs and generate 
the matching classes.

## Generation

The generator can generate _POJOs_ for composite types and enum classes for enumeration types. A simple example
can be seen in the generated files `UDTGeneratorTest.testGenerate()`

#### Referencing/Nested Types

The generator supports generating classes for SQL types that reference/contain other SQL types.
 
If the referenced type(s) have POJOs or enums being generated during the same execution, the generator will use 
the generated type value instead of the standard mapping.

If not, composite types will be created as `java.sql.Struct`s and enums will be created as `String`s. 

## Executing

The generator can be executed multiple ways.

### CLI
To execute the generator on the command line you can use the _uber jar_ as an executable JAR or by the standard
method of referencing the main class.

###### Uber JAR 

      java -jar udt-gen-complete-{ver}.jar [options] <type names>
      
###### Main Class

      java -classpath ... com.impossibl.postgres.tools.UGTGenerator [options] <type names>
      
##### Options
To generate classes from a schema the generator needs access to a running server with the schema available and 
a list of type names that it should generate classes for.

###### Generation
The options for class generation are as follows

* `-p`, `--pkg`         Target package for generated classes (Required)
* `-o`, `--out`         Output directory for generated files (defaults to `./out`)

After all other options on the command line a list of type names to generate must be provided. Each type can be
schema qualified and/or quoted as necessary for PostgreSQL's interpretation. For example:

      <exec generator> -o ./generated-classes/udts a_public_type public.b_public_type "MY_SCHEMA"."A_TYPE"

###### Connection
Uppercase short options are reserved for database connection information; standard driver defaults apply.

* `--url`               Database connection URL (overrides name, host, port)
* `-D`, `--database`    Database name
* `-H`, `--host`        Database server hostname
* `-T`, `--port`        Database server port
* `-U`, `--user`        Database username (can be used with `--url`)
* `-P`, `--password`    Database password (can be used with `--url`)

##### Library
To use the generator as a library and execute it programmatically.

```java
import com.impossibl.postgres.tools.UDTGenerator;
import java.sql.Connection;
import java.util.List;
import java.io.File;

class ExecGen {
  public static void executeGenerator(Connection connection, List<String> typeNames) {
    UDTGenerator.generate(connection, new File("out"), "sql.schema.types", typeNames);
  }
}
```
