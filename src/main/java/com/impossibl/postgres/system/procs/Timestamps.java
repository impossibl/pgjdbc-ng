package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.system.Settings.FIELD_DATETIME_FORMAT_CLASS;
import static com.impossibl.postgres.types.PrimitiveType.TimestampTZ;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.util.Calendar;
import java.util.TimeZone;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.datetime.TimeZones;
import com.impossibl.postgres.datetime.instants.AmbiguousInstant;
import com.impossibl.postgres.datetime.instants.FutureInfiniteInstant;
import com.impossibl.postgres.datetime.instants.Instant;
import com.impossibl.postgres.datetime.instants.PastInfiniteInstant;
import com.impossibl.postgres.datetime.instants.PreciseInstant;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;



public class Timestamps extends SettingSelectProcProvider {
	
	private static long PG_JAVA_EPOCH_DIFF_MICROS = calculateEpochDifferenceMicros();
	
	private TimeZone zone;
	private PrimitiveType primitiveType;

	public Timestamps(PrimitiveType primitiveType, String... baseNames) {
		super(FIELD_DATETIME_FORMAT_CLASS, Integer.class,
				null, null, null, null,
				null, null, null, null, baseNames);
		this.primitiveType = primitiveType;
		this.zone = primitiveType == TimestampTZ ? TimeZones.UTC : null;
		this.matchedBinEncoder = new BinIntegerEncoder();
		this.matchedBinDecoder = new BinIntegerDecoder();
	}

	class BinIntegerDecoder extends BinaryDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return primitiveType;
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
			
			if(micros == Long.MAX_VALUE) {
				return FutureInfiniteInstant.INSTANCE;
			}
			else if(micros == Long.MIN_VALUE) {
				return PastInfiniteInstant.INSTANCE;
			}
			
			micros += PG_JAVA_EPOCH_DIFF_MICROS;
			
			if(zone != null)
				return new PreciseInstant(Instant.Type.Timestamp, micros, zone);
			else
				return new AmbiguousInstant(Instant.Type.Timestamp, micros);
		}

	}

	class BinIntegerEncoder extends BinaryEncoder {

		public Class<?> getInputType() {
			return Instant.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return primitiveType;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			if (val == null) {

				buffer.writeInt(-1);
			}
			else {
				
				Instant inst = (Instant) val;
				val.toString();
				
				long micros;
				if(primitiveType == PrimitiveType.TimestampTZ) {
					micros = inst.getMicrosUTC();
				}
				else {
					micros = inst.disambiguate(TimeZone.getDefault()).getMicrosLocal();
				}
				
				if(!isInfinity(micros)) {
					
					micros -= PG_JAVA_EPOCH_DIFF_MICROS;
				}
				
				buffer.writeInt(8);
				buffer.writeLong(micros);
			}

		}

	}
	
	private static long calculateEpochDifferenceMicros() {
		
		Calendar pgEpochInJava = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

		pgEpochInJava.clear();
		pgEpochInJava.set(2000, 0, 1);
		
		return MILLISECONDS.toMicros(pgEpochInJava.getTimeInMillis());
	}

	public static boolean isInfinity(long micros) {
		
		return micros == Long.MAX_VALUE || micros == Long.MIN_VALUE;
	}
	
}
