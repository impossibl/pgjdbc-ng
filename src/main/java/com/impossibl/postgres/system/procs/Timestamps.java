package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.system.Settings.FIELD_DATETIME_FORMAT_CLASS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;



public class Timestamps extends SettingSelectProcProvider {

	private static long PG_JAVA_EPOCH_DIFF_MICROS = calculateEpochDifferenceMicros();

	public Timestamps() {
		super(FIELD_DATETIME_FORMAT_CLASS, Integer.class,
				null, null, new BinIntegerEncoder(), new BinIntegerDecoder(),
				null, null, null, null,
				"timestamp_", "timestamptz_");
	}

	static class BinIntegerDecoder implements Type.Codec.Decoder {

		public Class<?> getOutputType() {
			return Timestamp.class;
		}

		public Timestamp decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if (length == -1) {
				return null;
			}
			else if (length != 8) {
				throw new IOException("invalid length");
			}

			long microsPg = buffer.readLong();
			long microsJava = microsPg + PG_JAVA_EPOCH_DIFF_MICROS;

			return convertMicrosToTimestamp(microsJava);
		}

	}

	static class BinIntegerEncoder implements Type.Codec.Encoder {

		public Class<?> getInputType() {
			return Timestamp.class;
		}

		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			if (val == null) {

				buffer.writeInt(-1);
			}
			else {
				
				Timestamp ts = (Timestamp) val;
				
				long microsJava = convertTimestampToMicros(ts);
				long microsPg = microsJava - PG_JAVA_EPOCH_DIFF_MICROS;
				
				buffer.writeInt(8);
				buffer.writeLong(microsPg);
			}

		}

	}
	
	private static Timestamp convertMicrosToTimestamp(long micros) {
		
		long millis = MICROSECONDS.toMillis(micros);
		long leftoverMicros = micros - MILLISECONDS.toMicros(millis);
		
		Timestamp ts = new Timestamp(millis);
		
		long nanos = ts.getNanos() + MICROSECONDS.toNanos(leftoverMicros);
		ts.setNanos((int) nanos);
		return ts;
	}

	private static long convertTimestampToMicros(Timestamp timestamp) {
		
		long micros = MILLISECONDS.toMicros(timestamp.getTime());
		long extra = (timestamp.getNanos() % 1000000) / 1000;
		return micros + extra;
	}

	private static long calculateEpochDifferenceMicros() {
		
		Calendar pgEpochInJava = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

		pgEpochInJava.clear();
		pgEpochInJava.set(2000, 0, 1);
		
		return MILLISECONDS.toMicros(pgEpochInJava.getTimeInMillis());
	}

}
