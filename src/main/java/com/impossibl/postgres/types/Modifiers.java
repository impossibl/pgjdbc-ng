package com.impossibl.postgres.types;

import java.util.Map;

public class Modifiers {

	//Standard modifiers
	public static final String LENGTH = "length";
	public static final String SCALE = "scale";
	public static final String PRECISION = "precision";
	
	
	/**
	 * Parses a type modifier into a name=value hash map
	 */
	public interface Parser {
		Map<String, Object> parse(long mod);
	}

}
