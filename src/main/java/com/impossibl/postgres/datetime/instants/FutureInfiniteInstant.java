package com.impossibl.postgres.datetime.instants;

import com.impossibl.postgres.system.Context;


public class FutureInfiniteInstant extends InfiniteInstant {
	
	public static final FutureInfiniteInstant INSTANCE = new FutureInfiniteInstant();
	
	private FutureInfiniteInstant() {
	}
	
	@Override
	public long getMillisUTC() {
		return Long.MAX_VALUE;
	}

	@Override
	public long getMicrosUTC() {
		return Long.MAX_VALUE;
	}

	@Override
	public long getMicrosLocal() {
		return Long.MAX_VALUE;
	}

	@Override
	public long getMillisLocal() {
		return Long.MAX_VALUE;
	}

	@Override
	public String print(Context context) {
		return "infinity";
	}

}
