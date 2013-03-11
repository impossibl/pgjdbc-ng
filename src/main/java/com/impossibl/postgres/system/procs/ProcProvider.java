package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type.Codec;

public interface ProcProvider {
	
	public Codec.Encoder findEncoder(String name, Context context);
	public Codec.Decoder findDecoder(String name, Context context);

}
