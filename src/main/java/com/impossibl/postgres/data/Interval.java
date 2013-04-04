package com.impossibl.postgres.data;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Interval {
	
	private int months;
	private int days;
	private long timeMicros;
	
	public Interval(int months, int days, long timeMicros) {
		super();
		this.months = months;
		this.days = days;
		this.timeMicros = timeMicros;
	}

	public Interval(int years, int months, int days, int hours, int minutes, double seconds) {
		this.months += years * 12 + months;
		this.days = days;
		this.timeMicros += HOURS.toMicros(hours);
		this.timeMicros += MINUTES.toMicros(minutes);
		this.timeMicros += (long)(seconds * SECONDS.toMicros(1));
	}

	public Interval() {
	}

	public long getRawTime() {
		return timeMicros;
	}

	public void setRawTime(long time) {
		this.timeMicros = time;
	}

	public int getRawDays() {
		return days;
	}

	public void setRawDays(int days) {
		this.days = days;
	}

	public int getRawMonths() {
		return months;
	}

	public void setRawMonths(int months) {
		this.months = months;
	}

	public long getHours() {
		return MICROSECONDS.toHours(timeMicros);
	}

	public long getMinutes() {
		return MICROSECONDS.toMinutes(timeMicros) - HOURS.toMinutes(getHours());
	}

	public double getSeconds() {
		double microsDiff = ((double)timeMicros - (double)MINUTES.toMicros(MICROSECONDS.toMinutes(timeMicros)));
		microsDiff /= SECONDS.toMicros(1);
		return microsDiff;
	}

	public int getDays() {
		return days;
	}

	public int getMonths() {
		return months % 12;
	}

	public int getYears() {
		return months / 12;
	}

}
