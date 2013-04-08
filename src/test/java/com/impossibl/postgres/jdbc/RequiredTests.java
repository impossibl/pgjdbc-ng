package com.impossibl.postgres.jdbc;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
	SQLTextTests.class,
	ConnectionTest.class,
	DatabaseMetaDataTest.class,
	DatabaseMetaDataPropertiesTest.class,
	SavepointTest.class,
	StatementTest.class,
	PreparedStatementTest.class,
	ParameterMetaDataTest.class,
	GeneratedKeysTest.class,
	BatchExecuteTest.class,
	ResultSetTest.class,
	ResultSetMetaDataTest.class,
	ArrayTest.class,
	DateTest.class,
	TimestampTest.class,
	TimeTest.class,
	TimezoneTest.class,
	StructTest.class,
	BlobTest.class,
	XmlTest.class,
	IntervalTest.class,
	UUIDTest.class,
	WrapperTest.class,
	DriverTest.class,
})
public class RequiredTests {

}
