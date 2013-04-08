package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.types.PrimitiveType;

public class TimestampsWithoutTZ extends Timestamps {

	public TimestampsWithoutTZ() {
		super(PrimitiveType.Timestamp, "timestamp_");
	}

}
