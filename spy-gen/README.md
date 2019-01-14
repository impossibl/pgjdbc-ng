# SPY Code Generation

`SpyGen` generates Relay, Listener and Trace classes for all of the standard JDBC interface classes for the 
[Spy](../spy/README.md) library.

The code generator uses the [JDBC-API](../jdbc-api/README.md) library to generate classes with the original
parameter names from the original JDK source. To ensure the JVM loads the `java.sql` interfaces from this 
custom library, _spy-gen_ needs it placed on the boot classpath (using `Xbootclasspth/p` for Java 8 and 
`--patch-module` for Java 9+). 
