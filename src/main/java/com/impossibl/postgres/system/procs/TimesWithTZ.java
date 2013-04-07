package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.system.Settings.FIELD_DATETIME_FORMAT_CLASS;
import static com.impossibl.postgres.types.PrimitiveType.TimeTZ;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.util.TimeZone;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.datetime.TimeZones;
import com.impossibl.postgres.datetime.instants.Instant;
import com.impossibl.postgres.datetime.instants.PreciseInstant;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;



public class TimesWithTZ extends SettingSelectProcProvider {

	public TimesWithTZ() {
		super(FIELD_DATETIME_FORMAT_CLASS, Integer.class,
				null, null, new BinIntegerEncoder(), new BinIntegerDecoder(),
				null, null, null, null,
				"timetz_");
	}

	static class BinIntegerDecoder extends BinaryDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return TimeTZ;
		}
		
		public Class<?> getOutputType() {
			return Instant.class;
		}

		public Instant decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if (length == -1) {
				return null;
			}
			else if (length != 12) {
				throw new IOException("invalid length");
			}

			long micros = buffer.readLong();
			int tzOffsetSecs = buffer.readInt();
			
			int tzOffsetMillis = (int)SECONDS.toMillis(-tzOffsetSecs);
			TimeZone zone = TimeZones.getOffsetZone(tzOffsetMillis);
			
			return new PreciseInstant(Instant.Type.Time, micros, zone);
		}

	}

	static class BinIntegerEncoder extends BinaryEncoder {

		public Class<?> getInputType() {
			return Instant.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return TimeTZ;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			if (val == null) {

				buffer.writeInt(-1);
			}
			else {
				
				Instant inst = (Instant) val;
				
				long micros = inst.getMicrosLocal() % DAYS.toMicros(1);
				
				int tzOffsetSecs = (int) -inst.getZoneOffsetSecs();
				
				buffer.writeInt(12);
				buffer.writeLong(micros);
				buffer.writeInt(tzOffsetSecs);
			}

		}

	}
	
}
