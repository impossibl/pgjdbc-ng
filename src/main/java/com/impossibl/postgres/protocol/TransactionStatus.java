package com.impossibl.postgres.protocol;

public enum TransactionStatus {
	
	Idle		('I'),
	Active	('T'),
	Failed	('E')
	;
	
	byte code;
	
	TransactionStatus(char code) {
		this.code = (byte)code;
	}

	public byte getCode() {
		return code;
	}


}
