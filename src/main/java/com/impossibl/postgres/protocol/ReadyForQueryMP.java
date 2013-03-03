package com.impossibl.postgres.protocol;

import static com.impossibl.postgres.protocol.TransactionStatus.Active;
import static com.impossibl.postgres.protocol.TransactionStatus.Failed;
import static com.impossibl.postgres.protocol.TransactionStatus.Idle;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.utils.DataInputStream;

public class ReadyForQueryMP implements MessageProcessor {

	@Override
	public void process(DataInputStream in, Context context) throws IOException {

		TransactionStatus txStatus;
		
		switch(in.readByte()) {
		case 'T':
			txStatus = Active;
			break;
		case 'F':
			txStatus = Failed;
			break;
		case 'I':
			txStatus = Idle;
			break;
		default:
			throw new IllegalStateException("invalid transaction status");
		}
		
		context.restart(txStatus);
	}

}
