# Build Tools

## Versions

All version information, including Gradle plugin versions, are managed in the `Versions` object.
`Versions`, being declared in this `buildSrc` project, means that it is available in all sections
(buildscript, plugins, etc.) of all projects/modules with exception of `buildSrc` itself.

## SpyGen
`SpyGen` generates Relay, Listener and Trace classes for all of the standard JDBC interfaces for the 
[Spy](../spy/README.md) library.
