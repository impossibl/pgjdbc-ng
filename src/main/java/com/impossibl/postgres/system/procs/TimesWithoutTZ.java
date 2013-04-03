package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.system.Settings.FIELD_DATETIME_FORMAT_CLASS;
import static com.impossibl.postgres.types.PrimitiveType.Time;
import static java.util.concurrent.TimeUnit.DAYS;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.datetime.instants.AmbiguousInstant;
import com.impossibl.postgres.datetime.instants.Instant;
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
			return Instant.class;
		}

		public Instant decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if (length == -1) {
				return null;
			}
			else if (length != 8) {
				throw new IOException("invalid length");
			}

			long micros = buffer.readLong();
			
			return new AmbiguousInstant(Instant.Type.Time, micros);
		}

	}

	static class BinIntegerEncoder implements Type.Codec.Encoder {

		public Class<?> getInputType() {
			return Instant.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Time;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			if (val == null) {

				buffer.writeInt(-1);
			}
			else {
				
				Instant inst = (Instant) val;

				long micros = inst.getMicrosLocal() % DAYS.toMicros(1);
				
				buffer.writeInt(8);
				buffer.writeLong(micros);
			}

		}

	}
	
}
