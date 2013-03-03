package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.types.Type.BinaryIO;
import com.impossibl.postgres.types.Type.TextIO;


public class Procs {
	
	public static ProcProvider[] PROVIDERS = {
		new Arrays(),
		new Bools(),
		new Bytes(),
		new Chars(),
		new Float4s(),
		new Float8s(),
		new Int2s(),
		new Int4s(),
		new Int8s(),
		new Names(),
		new Strings(),
		new UUIDs(),
		new Records()
	};

	public static TextIO loadNamerTextIO(String baseName) {
		TextIO io = new TextIO();
		io.encoder = loadInputProc(baseName + "in");
		io.decoder = loadOutputProc(baseName + "out");
		return io;
	}

	public static TextIO.Encoder loadInputProc(String name) {

		TextIO.Encoder h;
		
		for(ProcProvider pp : Procs.PROVIDERS) {
			if((h = pp.findTextEncoder(name)) != null)
				return h;
		}

		return null;
	}

	public static TextIO.Decoder loadOutputProc(String name) {
		
		TextIO.Decoder h;
		
		for(ProcProvider pp : Procs.PROVIDERS) {
			if((h = pp.findTextDecoder(name)) != null)
				return h;
		}
		
		return null;
	}
	
	public static BinaryIO loadNamedBinaryIO(String baseName) {
		BinaryIO io = new BinaryIO();
		io.encoder = loadReceiveProc(baseName + "recv");
		io.decoder = loadSendProc(baseName + "send");
		return io;
	}

	public static BinaryIO.Encoder loadReceiveProc(String name) {
		
		BinaryIO.Encoder h;
		
		for(ProcProvider pp : Procs.PROVIDERS) {
			if((h = pp.findBinaryEncoder(name)) != null)
				return h;
		}

		return null;
	}

	public static BinaryIO.Decoder loadSendProc(String name) {
		
		BinaryIO.Decoder h;
		
		for(ProcProvider pp : Procs.PROVIDERS) {
			if((h = pp.findBinaryDecoder(name)) != null)
				return h;
		}

		return null;
	}

}
