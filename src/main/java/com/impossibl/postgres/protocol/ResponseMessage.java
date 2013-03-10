package com.impossibl.postgres.protocol;

import org.jboss.netty.buffer.ChannelBuffer;

public class ResponseMessage {
	
	public byte id;
	public ChannelBuffer data;

	public ResponseMessage(byte id, ChannelBuffer data) {
		super();
		this.id = id;
		this.data = data;
	}

}
