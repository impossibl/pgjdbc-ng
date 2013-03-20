package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.jdbc.ErrorUtils.makeSQLException;
import static com.impossibl.postgres.system.Settings.CREDENTIALS_PASSWORD;
import static com.impossibl.postgres.system.Settings.CREDENTIALS_USERNAME;
import static com.impossibl.postgres.system.Settings.DATABASE_URL;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.NoticeException;
import com.impossibl.postgres.system.Version;


public class PGDriver implements Driver {
	
	public static final Version VERSION = Version.get(0, 1, 0);
	
	private static final String JDBC_USERNAME_PARAM = "user";
	private static final String JDBC_PASSWORD_PARAM = "password";

	static class ConnectionSpecifier {
		public String hostname;
		public Integer port;
		public String database;
		public Properties parameters = new Properties();
	}

	
	
	
	public PGDriver() throws SQLException {
		DriverManager.registerDriver(this);
	}

	@Override
	public PGConnection connect(String url, Properties info) throws SQLException {
		
		ConnectionSpecifier connSpec = parseURL(url);
		if(connSpec == null) {
			return null;
		}

		try {

			Properties settings = buildSettings(connSpec, info);
			
			SocketAddress address = new InetSocketAddress(connSpec.hostname, connSpec.port);
			
			PGConnection conn = new PGConnection(address, settings, Collections.<String, Class<?>>emptyMap());
			
			conn.init();
			
			return conn;
			
		}
		catch (IOException e) {
			
			throw new SQLException("Connection Error", e);
		}
		catch (NoticeException e) {
			
			throw makeSQLException(e.getNotice());
		}
		
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return parseURL(url) != null;
	}
	
	/**
	 * Combines multiple sources of properties into one group. Connection info
	 * parameters take precedence over URL query parameters. Also, it ensure
	 * that all required parameters has some default value. 
	 * 
	 * @param connSpec Connection specification as parsed
	 * @param connectInfo Connection info properties passed to connect
	 * @return Single group of settings
	 */
	Properties buildSettings(ConnectionSpecifier connSpec, Properties connectInfo) {
		
		Properties settings = new Properties();
		
		//Start by adding all parameters from the URL query string
		settings.putAll(connSpec.parameters);
		
		//Add (or overwrite) parameters from the connection info
		settings.putAll(connectInfo);
		
		//Set PostgreSQL's database parameter from connSpec
		settings.put("database", connSpec.database);
		
		//Translate JDBC parameters to PostgreSQL parameters
		settings.put(CREDENTIALS_USERNAME, settings.getProperty(JDBC_USERNAME_PARAM, ""));
		settings.put(CREDENTIALS_PASSWORD, settings.getProperty(JDBC_PASSWORD_PARAM, ""));
		
		settings.put(DATABASE_URL, "jdbc:postgresql://" + connSpec.hostname + "/" + connSpec.database);		
		
		return settings;
	}

	/*
	 * URL Pattern jdbc:postgresql:(?://(?:([a-zA-Z0-9\-\.]+|\[[0-9a-f\:]+\])(?:\:(\d+))?)/)?(\w+)(?:\?(.*))?
	 * 	Capturing Groups:
	 * 		1 = host name, IPv4, IPv6	(optional)
	 * 		2 = port 									(optional)
	 * 		3 = database name					(required)
	 * 		4 = parameters						(optional)
	 */
	private static final Pattern URL_PATTERN = Pattern.compile("jdbc:postgresql:(?://(?:([a-zA-Z0-9\\-\\.]+|\\[[0-9a-f\\:]+\\])(?:\\:(\\d+))?)/)?(\\w+)(?:\\?(.*))?");
	
	/**
	 * Parses a URL connection string.
	 * 
	 * Uses the URL_PATTERN to capture a hostname or ip address, port, database
	 * name and a list of parameters specified as query name=value pairs. All
	 * parts but the database name are optional.
	 * 
	 * @param url
	 * @return
	 */
	private ConnectionSpecifier parseURL(String url) {
		
		try {
			
			//First match aginst the entire URL pattern.  If that doesn't work
			//then the url is invalid
			
			Matcher urlMatcher = URL_PATTERN.matcher(url);
			if (!urlMatcher.matches()) {
				return null;
			}
			
			//Now build a conn-spec from the optional pieces of the URL
			//
			
			ConnectionSpecifier spec = new ConnectionSpecifier();
			
			//Assign hostname, if provided, or use the default "localhost"
			
			spec.hostname = urlMatcher.group(1);
			if(spec.hostname == null || spec.hostname.isEmpty()) {
				spec.hostname = "localhost";
			}
			
			//Assign port, if provided, or use the default "5432"
			
			String port = urlMatcher.group(2);
			if(port != null && !port.isEmpty()) {
				spec.port = Integer.parseInt(port);
			}
			else {
				spec.port = 5432;
			}
			
			//Assign the database
			
			spec.database = urlMatcher.group(3);
			
			//Parse the query string as a list of name=value pairs separated by '&'
			//then assign them as extra parameters
			
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
		
		List<DriverPropertyInfo> propInfo = new ArrayList<>();
		
		ConnectionSpecifier spec = parseURL(url);
		if(spec == null)
			spec = new ConnectionSpecifier();
		
		if(spec.database == null || spec.database.isEmpty())
			propInfo.add(new DriverPropertyInfo("database",""));			
		if(spec.parameters.get("username") == null || spec.parameters.get("username").toString().isEmpty())
			propInfo.add(new DriverPropertyInfo("username",""));			
		if(spec.parameters.get("password") == null || spec.parameters.get("password").toString().isEmpty())
			propInfo.add(new DriverPropertyInfo("password",""));			
		
		return propInfo.toArray(new DriverPropertyInfo[propInfo.size()]);
	}

	@Override
	public int getMajorVersion() {
		return 0;
	}

	@Override
	public int getMinorVersion() {
		return 1;
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
