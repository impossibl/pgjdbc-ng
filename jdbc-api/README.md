# JDBC API

This library contains standard JDK SQL interfaces compiled with parameter name information. It is only used as input 
to [spy-gen](../spy-gen/README.md) to generate the required classes for [spy](../spy/README.md) with correct parameter
names; it only contains the subset of interfaces used needed by the generator.  

It is **NOT** included in the Driver or Spy libraries in any fashion; its merely a means to get parameter names 
into tracing information.  
