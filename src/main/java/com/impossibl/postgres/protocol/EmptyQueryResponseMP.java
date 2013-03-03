package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.utils.DataInputStream;

public class EmptyQueryResponseMP implements MessageProcessor {

	@Override
	public void process(DataInputStream in, Context context) throws IOException {
	}

}
