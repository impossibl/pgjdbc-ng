package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.types.Type.BinaryIO;
import com.impossibl.postgres.types.Type.TextIO;

public interface ProcProvider {
	
	public BinaryIO.Encoder findBinaryEncoder(String name);
	public BinaryIO.Decoder findBinaryDecoder(String name);
	public TextIO.Encoder findTextEncoder(String name);
	public TextIO.Decoder findTextDecoder(String name);

}
