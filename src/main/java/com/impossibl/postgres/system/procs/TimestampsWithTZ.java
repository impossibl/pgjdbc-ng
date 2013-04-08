package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.types.PrimitiveType;

public class TimestampsWithTZ extends Timestamps {

	public TimestampsWithTZ() {
		super(PrimitiveType.TimestampTZ, "timestamptz_");
	}

}
