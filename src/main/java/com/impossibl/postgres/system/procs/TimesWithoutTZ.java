package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.system.Settings.FIELD_DATETIME_FORMAT_CLASS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.sql.Time;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;



public class TimesWithoutTZ extends SettingSelectProcProvider {

	public TimesWithoutTZ() {
		super(FIELD_DATETIME_FORMAT_CLASS, Integer.class,
				null, null, new BinIntegerEncoder(), new BinIntegerDecoder(),
				null, null, null, null,
				"time_");
	}

	static class BinIntegerDecoder implements Type.Codec.Decoder {

		public Time decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if (length == -1) {
				return null;
			}
			else if (length != 8) {
				throw new IOException("invalid length");
			}

			long micros = buffer.readLong();
			
			long millis = MICROSECONDS.toMillis(micros);
			
			return new Time(millis);
		}

	}

	static class BinIntegerEncoder implements Type.Codec.Encoder {

		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			if (val == null) {

				buffer.writeInt(-1);
			}
			else {
				
				Time time = (Time) val;
				
				long millis = time.getTime();
				
				long micros = MILLISECONDS.toMicros(millis);
				
				buffer.writeInt(8);
				buffer.writeLong(micros);
			}

		}

	}
	
}
