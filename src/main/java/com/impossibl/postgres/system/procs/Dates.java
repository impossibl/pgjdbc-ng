package com.impossibl.postgres.system.procs;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.sql.Date;
import java.util.Calendar;
import java.util.TimeZone;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;



public class Dates extends SimpleProcProvider {

	private static final long PG_JAVA_EPOCH_DIFF_MILLIS = calculateEpochDifferenceMillis();


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
			long millisPg = DAYS.toMillis(daysPg);
			long millisJava = millisPg + PG_JAVA_EPOCH_DIFF_MILLIS;
			
			return new Date(millisJava);
		}

	}

	static class Encoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			if (val == null) {
				
				stream.writeInt(-1);
			}
			else {
				
				Date date = (Date) val;
								
				long millisJava = date.getTime();				
				long millisPg = millisJava - PG_JAVA_EPOCH_DIFF_MILLIS;
				int daysPg = (int) MILLISECONDS.toDays(millisPg);
				
				stream.writeInt(4);
				stream.writeInt(daysPg);
			}
			
		}

	}
	
	private static long calculateEpochDifferenceMillis() {
		
		Calendar pgEpochInJava = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

		pgEpochInJava.clear();
		pgEpochInJava.set(2000, 0, 1);
		
		return pgEpochInJava.getTimeInMillis();
	}

}
