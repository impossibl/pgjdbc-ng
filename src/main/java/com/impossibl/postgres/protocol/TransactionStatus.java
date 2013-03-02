package com.impossibl.postgres.protocol;

public enum TransactionStatus {
	
	Idle		('I'),
	Active	('T'),
	Failed	('F')
	;
	
	byte code;
	
	TransactionStatus(char code) {
		this.code = (byte)code;
	}

	public byte getCode() {
		return code;
	}


}
