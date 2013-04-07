package com.impossibl.postgres.jdbc;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
	ArrayTest.class,
	BatchExecuteTest.class,
	BlobTest.class,
	ConnectionTest.class,
	DatabaseMetaDataPropertiesTest.class,
	DatabaseMetaDataTest.class,
	DateTest.class,
	DriverTest.class,
	GeneratedKeysTest.class,
	IntervalTest.class,
	ParameterMetaDataTest.class,
	PreparedStatementTest.class,
	ResultSetMetaDataTest.class,
	ResultSetTest.class,
	SavepointTest.class,
	SQLTextTests.class,
	StatementTest.class,
	TimestampTest.class,
	TimeTest.class,
	TimezoneTest.class,
	UUIDTest.class,
	WrapperTest.class,
	XmlTest.class,
})
public class RequiredTests {

}
