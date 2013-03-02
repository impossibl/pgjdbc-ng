package com.impossibl.postgres.system.procs;


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
		new Records(),
		new Strings(),
		new UUIDs()
	};

}
