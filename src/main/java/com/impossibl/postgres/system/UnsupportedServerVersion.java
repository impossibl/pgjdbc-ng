package com.impossibl.postgres.system;

public class UnsupportedServerVersion extends IllegalStateException {

	public UnsupportedServerVersion(Version version) {
		super("An attempt was made to use an unsupported server version: " + version);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 4053890602809888396L;

}
