package com.impossibl.postgres.datetime.instants;

import com.impossibl.postgres.system.Context;


public class PastInfiniteInstant extends InfiniteInstant {
	
	public static final PastInfiniteInstant INSTANCE = new PastInfiniteInstant();
	
	private PastInfiniteInstant() {
	}

	@Override
	public long getMillisUTC() {
		return Long.MIN_VALUE;
	}

	@Override
	public long getMicrosUTC() {
		return Long.MIN_VALUE;
	}

	@Override
	public long getMicrosLocal() {
		return Long.MIN_VALUE;
	}

	@Override
	public long getMillisLocal() {
		return Long.MIN_VALUE;
	}
	
	@Override
	public String print(Context context) {
		return "-infinity";
	}

}
