package com.impossibl.postgres.system.procs;

import java.io.IOException;
import java.sql.Date;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;



public class Dates extends SimpleProcProvider {

	private static final long PG_JAVA_EPOCH_DIFF_DAYS = calculateEpochDifferenceDays();


	public Dates() {
		super(null, null, new Encoder(), new Decoder(), "date_");
	}

	static class Decoder implements Type.BinaryIO.Decoder {

		public Date decode(Type type, DataInputStream stream, Context context) throws IOException {

			int length = stream.readInt();
			if(length == -1) {
				return null;
			}
			else if (length != 4) {
				throw new IOException("invalid length");
			}
			
			int daysPg = stream.readInt();
			long daysJava = daysPg + PG_JAVA_EPOCH_DIFF_DAYS;
			long timeJava = TimeUnit.DAYS.toMillis(daysJava);
			
			return new Date(timeJava);
		}

	}

	static class Encoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			if (val == null) {
				
				stream.writeInt(-1);
			}
			else {
				
				Date date = (Date) val;
				
				long timeJava = date.getTime();
				long daysJava = TimeUnit.MILLISECONDS.toDays(timeJava);
				int daysPg = (int)(daysJava - PG_JAVA_EPOCH_DIFF_DAYS);
				
				stream.writeInt(4);
				stream.writeInt(daysPg);
			}
			
		}

	}
	
	private static long calculateEpochDifferenceDays() {
		
		Calendar pgEpochInJava = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

		pgEpochInJava.clear();
		pgEpochInJava.set(2000, 0, 1);
		
		return TimeUnit.MILLISECONDS.toDays(pgEpochInJava.getTimeInMillis());
	}

}
