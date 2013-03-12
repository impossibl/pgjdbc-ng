package com.impossibl.postgres.protocol;

public enum ServerObjectType {
	
	Statement	('S'),
	Portal		('P');
	
	byte id;
	ServerObjectType(char id) { this.id = (byte)id; }
	public byte getId() { return id; }
}

