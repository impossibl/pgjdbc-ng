package com.impossibl.postgres.codecs;

import java.sql.Date;
import java.text.DateFormat;
import java.util.TimeZone;

public class DateTimeCodec implements Codec<Date> {
	
	private DateFormat format;
	private TimeZone timeZone;
	
	public DateTimeCodec(DateFormat format, TimeZone timeZone) {
		this.format = format;
		this.timeZone = timeZone;
	}

	@Override
	public Date decode(byte[] value) {
		return null;
	}

	@Override
	public byte[] encode(String value) {
		return null;
	}

	public DateFormat getFormat() {
		return format;
	}

	public void setFormat(DateFormat format) {
		this.format = format;
	}

	public TimeZone getTimeZone() {
		return timeZone;
	}

	public void setTimeZone(TimeZone timeZone) {
		this.timeZone = timeZone;
	}

}
