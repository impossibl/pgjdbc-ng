package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.utils.DataInputStream;

public class NoticeResponseMP implements MessageProcessor {

	@Override
	public void process(DataInputStream in, ResponseHandler handler) throws IOException {

		byte type;
		
		while((type = in.readByte()) != 0) {
			
			String value = in.readCString();
			
			handler.getContext().reportNotice(type, value);
		}
		
	}

}
