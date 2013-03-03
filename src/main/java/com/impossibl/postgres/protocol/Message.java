package com.impossibl.postgres.protocol;

import java.io.ByteArrayOutputStream;

import com.impossibl.postgres.utils.DataOutputStream;

public class Message extends DataOutputStream {
	
	private byte id;

	public Message(byte id) {
		super(new ByteArrayOutputStream());
		this.id = id;
	}

	public byte getId() {
		return id;
	}

	public void setId(byte id) {
		this.id = id;
	}

	public ByteArrayOutputStream getData() {
		return (ByteArrayOutputStream) this.out;
	}

}
