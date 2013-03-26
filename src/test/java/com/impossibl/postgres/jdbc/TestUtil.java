package com.impossibl.postgres.jdbc;

import static com.google.common.base.Preconditions.checkArgument;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Joiner;

public class TestUtil {
	
	Connection open() throws SQLException {
		return DriverManager.getConnection(getURL(), getProperties());
	}

	public static String getURL(Object... urlParams) {
		
		String query = "";
		if(urlParams != null && urlParams.length > 0) {
			query = "?" + Joiner.on("&").withKeyValueSeparator("=").join(params(urlParams));
		}
		
		return "jdbc:postgresql://" + getServer() + "/" + getDatabase() + query;
	}

	public static String getServer() {
		return System.getProperty("pgjdbc.test.server","test");
	}

	public static String getPort() {
		return System.getProperty("pgjdbc.test.port","5432");
	}

	public static String getDatabase() {
		return System.getProperty("pgjdbc.test.db","test");
	}

	public static Properties getProperties() {

		Properties props = new Properties();
		
		props.setProperty("user", getUser());
		props.setProperty("password", getPassword());
		
		return props;
	}

	public static String getUser() {
		return System.getProperty("pgjdbc.test.user", "postgres");
	}

	public static String getPassword() {
		return System.getProperty("pgjdbc.test.password", "test");
	}

	private static Map<String, Object> params(Object... objs) {
		
		checkArgument(objs.length % 2 == 0);
		
		Map<String, Object> map = new HashMap<>();
		for(int c=0; c < objs.length; c+=2)
			map.put((String)objs[c], objs[c+1]);
		
		return map;
	}
	
}
