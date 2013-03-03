package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.utils.DataInputStream;

public class NoticeResponseMP implements MessageProcessor {

	@Override
	public void process(DataInputStream in, Context context) throws IOException {

		byte type;
		
		while((type = in.readByte()) != 0) {
			
			String value = in.readCString();
			
			context.reportNotice(type, value);
		}
		
	}

}
