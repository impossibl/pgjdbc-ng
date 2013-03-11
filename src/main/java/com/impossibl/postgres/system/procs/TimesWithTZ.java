package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.system.Settings.FIELD_DATETIME_FORMAT_CLASS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.sql.Time;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;



public class TimesWithTZ extends SettingSelectProcProvider {

	public TimesWithTZ() {
		super(FIELD_DATETIME_FORMAT_CLASS, Integer.class,
				null, null, new BinIntegerEncoder(), new BinIntegerDecoder(),
				null, null, null, null,
				"timetz_");
	}

	static class BinIntegerDecoder implements Type.BinaryIO.Decoder {

		public Time decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if (length == -1) {
				return null;
			}
			else if (length != 12) {
				throw new IOException("invalid length");
			}

			long baseMicros = buffer.readLong();
			int tzOffsetSecs = buffer.readInt();
			
			long micros = baseMicros+ SECONDS.toMicros(tzOffsetSecs);
			
			long millis = MICROSECONDS.toMillis(micros);
			
			return new Time(millis);
		}

	}

	static class BinIntegerEncoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			if (val == null) {

				buffer.writeInt(-1);
			}
			else {
				
				Time time = (Time) val;
				
				long millis = time.getTime();
				
				long baseMicros = MILLISECONDS.toMicros(millis);
				int tzOffsetSecs = (int)MILLISECONDS.toSeconds(context.getTimeZone().getRawOffset());
				
				buffer.writeInt(12);
				buffer.writeLong(baseMicros);
				buffer.writeInt(tzOffsetSecs);
			}

		}

	}
	
}
