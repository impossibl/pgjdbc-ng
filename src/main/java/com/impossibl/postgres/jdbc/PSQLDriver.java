package com.impossibl.postgres.jdbc;

import java.io.IOException;
import java.net.Socket;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.impossibl.postgres.Context;


public class PSQLDriver implements Driver {
	
	/*
	 * URL Pattern jdbc:postgresql:(?://(?:([a-zA-Z0-9\-\.]+|\[[0-9a-f\:]+\])(?:\:(\d+))?)/)?(\w+)(?:\?(.*))?
	 * 	Capturing Groups:
	 * 		1 = host name, IPv4, IPv6	(optional)
	 * 		2 = port 									(optional)
	 * 		3 = database name					(required)
	 * 		4 = parameters						(optional)
	 */
	private static final Pattern URL_PATTERN = Pattern.compile("jdbc:postgresql:(?://(?:([a-zA-Z0-9\\-\\.]+|\\[[0-9a-f\\:]+\\])(?:\\:(\\d+))?)/)?(\\w+)(?:\\?(.*))?");
	
	
	static class ConnectionSpecifier {
		public String hostname;
		public Integer port;
		public String database;
		public Map<String, String> parameters = new HashMap<String, String>();
	}
	
	
	public PSQLDriver() throws SQLException {
		DriverManager.registerDriver(this);
	}

	@Override
	public PSQLConnection connect(String url, Properties info) throws SQLException {
		
		ConnectionSpecifier connSpec = parseURL(url);
		if(connSpec == null) {
			return null;
		}
		
		try {
			
			Socket socket = new Socket(connSpec.hostname, connSpec.port);
			
			PSQLConnection conn = new PSQLConnection(socket, info, Collections.<String, Class<?>>emptyMap());
			
			conn.init();
			
			return conn;
			
		}
		catch (IOException e) {
			
			throw new SQLException("Connection Error", e);
		}
		
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return parseURL(url) != null;
	}

	private ConnectionSpecifier parseURL(String url) {
		
		try {
			
			Matcher urlMatcher = URL_PATTERN.matcher(url);
			if (!urlMatcher.find()) {
				return null;
			}
			
			ConnectionSpecifier spec = new ConnectionSpecifier();
			
			spec.hostname = urlMatcher.group(1);
			if(spec.hostname == null || spec.hostname.isEmpty()) {
				spec.hostname = "localhost";
			}
			
			String port = urlMatcher.group(2);
			if(port != null && !port.isEmpty()) {
				spec.port = Integer.parseInt(port);
			}
			else {
				spec.port = 5432;
			}
			
			spec.database = urlMatcher.group(3);
			
			String params = urlMatcher.group(4);
			if(params != null && !params.isEmpty()) {
				
				for(String nameValue : params.split("&")) {
					
					String[] items = nameValue.split("=");
					
					if(items.length == 1) {
						spec.parameters.put(items[0],null);
					}
					else if(items.length == 2) {
						spec.parameters.put(items[0],items[1]);
					}
					
				}
			}
			
			return spec;
			
		}
		catch(Throwable e) {
			return null;
		}
		
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getMajorVersion() {
		return 1;
	}

	@Override
	public int getMinorVersion() {
		return 0;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return Logger.getLogger(Context.class.getPackage().getName());
	}

}
