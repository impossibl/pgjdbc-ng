# Spy - JDBC API Level Tracing

Spy is an API level tracing facility using auto-generated relay & listener classes for each JDBC API interface. A
tracing listener is also generated for each interface that makes trace logging easy to implement.


Although the library is currently used as an API level tracing system for the `pgjdbc-ng` driver it should be
able to be used with any compliant JDBC driver. Also, implementing your own listener classes allows you to 
implement a whole lot more than just tracing.
   

To use the library as a API tracing system you have three options:

1. Manually wrap the a connection returned from a JDBC `Driver` or `DataSource` and use the wrapped connection 
   as normal
    
       Connection conn = ConnectionTracer(DriverManager.getConnection(...), FileTracer("api.trace"))

2. Use the `SpyDriver` by the prepending `:spy` to the driver url scheme and passing trace related settings
   via driver properties. For example, given the regular url scheme of `jdbc:pgjdbc` you would use
   `jdbc:spy:pgjdbc` as follows:

        Properties props = new Properties();
        props.setProperty("spy.tracer.file", "api.trace");
        Connection conn = DriverManager.getConnection("jdbc:spy:pgjdbc://localhost/db", props);

3. Use a driver that already has it implemented internally like `pgjdbc-ng` ;)
