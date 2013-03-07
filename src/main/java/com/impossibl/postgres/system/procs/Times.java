package com.impossibl.postgres.system.procs;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;



public class Times extends SettingSelectProcProvider {

	private static long PG_JAVA_EPOCH_DIFF_MICROS = calculateEpochDifferenceMicros();

	public Times() {
		super("datetimes.binary.class", Integer.class,
				null, null, new BinIntegerEncoder(), new BinIntegerDecoder(),
				null, null, null, null,
				"time_", "timetz_");
	}

	static class BinIntegerDecoder implements Type.BinaryIO.Decoder {

		public Timestamp decode(Type type, DataInputStream stream, Context context) throws IOException {

			int length = stream.readInt();
			if (length == -1) {
				return null;
			}
			else if (length != 8) {
				throw new IOException("invalid length");
			}

			long microsPg = stream.readLong();
			long microsJava = microsPg + PG_JAVA_EPOCH_DIFF_MICROS;

			return convertMicrosToTimestamp(microsJava);
		}

	}

	static class BinIntegerEncoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			if (val == null) {

				stream.writeInt(-1);
			}
			else {
				
				Timestamp ts = (Timestamp) val;
				
				long microsJava = convertTimestampToMicros(ts);
				long microsPg = microsJava - PG_JAVA_EPOCH_DIFF_MICROS;
				
				stream.writeInt(8);
				stream.writeLong(microsPg);
			}

		}

	}
	
	private static Timestamp convertMicrosToTimestamp(long micros) {
		
		long millis = TimeUnit.MICROSECONDS.toMillis(micros);
		long leftoverMicros = micros - TimeUnit.MILLISECONDS.toMicros(millis);
		
		Timestamp ts = new Timestamp(millis);
		
		long nanos = ts.getNanos() + TimeUnit.MICROSECONDS.toNanos(leftoverMicros);
		ts.setNanos((int) nanos);
		return ts;
	}

	private static long convertTimestampToMicros(Timestamp timestamp) {
		
		long micros = TimeUnit.MILLISECONDS.toMicros(timestamp.getTime());
		long extra = (timestamp.getNanos() % 1000000) / 1000;
		return micros + extra;
	}

	private static long calculateEpochDifferenceMicros() {
		
		Calendar pgEpochInJava = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

		pgEpochInJava.clear();
		pgEpochInJava.set(2000, 0, 1);
		
		return TimeUnit.MILLISECONDS.toMicros(pgEpochInJava.getTimeInMillis());
	}

}
