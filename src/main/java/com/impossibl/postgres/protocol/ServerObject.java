package com.impossibl.postgres.protocol;

public enum ServerObject {
	
	Statement	('S'),
	Portal		('P');
	
	byte id;
	ServerObject(char id) { this.id = (byte)id; }
	public byte getId() { return id; }
}

