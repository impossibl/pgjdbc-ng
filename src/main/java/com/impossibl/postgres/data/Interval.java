/**
 * Copyright (c) 2013, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
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
