package com.impossibl.postgres.jdbc;

public class Identifiers {
	
	public static String escape(String identifier) {
		
		StringBuilder sb = new StringBuilder();
		sb.append('"');
		sb.append(identifier.replaceAll("\"", "\"\""));
		sb.append('"');
		return sb.toString();
	}

}
