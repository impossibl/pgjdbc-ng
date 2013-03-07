package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type.BinaryIO;
import com.impossibl.postgres.types.Type.TextIO;

public interface ProcProvider {
	
	public BinaryIO.Encoder findBinaryEncoder(String name, Context context);
	public BinaryIO.Decoder findBinaryDecoder(String name, Context context);
	public TextIO.Encoder findTextEncoder(String name, Context context);
	public TextIO.Decoder findTextDecoder(String name, Context context);

}
