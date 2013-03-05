package com.impossibl.postgres.utils;

public class Timer {
	
	long start;
	long lapStart;
	
	public Timer() {
		this.start = this.lapStart = System.currentTimeMillis();
	}
	
	public long getLap() {
		long lap = (System.currentTimeMillis() - lapStart);
		lapStart = System.currentTimeMillis();
		return lap;
	}
	
	public long getTotal() {
		return (System.currentTimeMillis() - start);
	}

}
