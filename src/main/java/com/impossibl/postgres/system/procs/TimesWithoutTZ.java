package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.system.Settings.FIELD_DATETIME_FORMAT_CLASS;
import static com.impossibl.postgres.types.PrimitiveType.Time;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.joda.time.DateTimeZone.UTC;

import java.io.IOException;
import java.sql.Time;

import org.jboss.netty.buffer.ChannelBuffer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;



public class TimesWithoutTZ extends SettingSelectProcProvider {

	public TimesWithoutTZ() {
		super(FIELD_DATETIME_FORMAT_CLASS, Integer.class,
				null, null, new BinIntegerEncoder(), new BinIntegerDecoder(),
				null, null, null, null,
				"time_");
	}

	static class BinIntegerDecoder implements Type.Codec.Decoder {

		public PrimitiveType getInputPrimitiveType() {
			return Time;
		}
		
		public Class<?> getOutputType() {
			return Time.class;
		}

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
			
			DateTime dt = new DateTime(millis, UTC).withZoneRetainFields(DateTimeZone.getDefault());
			
			return new Time(dt.getMillis());
		}

	}

	static class BinIntegerEncoder implements Type.Codec.Encoder {

		public Class<?> getInputType() {
			return Time.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Time;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			if (val == null) {

				buffer.writeInt(-1);
			}
			else {
				
				DateTime time = new DateTime(val).withZoneRetainFields(UTC);
				
				long millis = time.getMillis();
				
				long micros = MILLISECONDS.toMicros(millis);
				
				buffer.writeInt(8);
				buffer.writeLong(micros);
			}

		}

	}
	
}
