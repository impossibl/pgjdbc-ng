package com.impossibl.postgres.system.procs;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.sql.Date;
import java.util.Calendar;
import java.util.TimeZone;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;



public class Dates extends SimpleProcProvider {

	private static final long PG_JAVA_EPOCH_DIFF_MILLIS = calculateEpochDifferenceMillis();


	public Dates() {
		super(null, null, new Encoder(), new Decoder(), "date_");
	}

	static class Decoder implements Type.Codec.Decoder {

		public Class<?> getOutputType() {
			return Date.class;
		}

		public Date decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if(length == -1) {
				return null;
			}
			else if (length != 4) {
				throw new IOException("invalid length");
			}
			
			int daysPg = buffer.readInt();
			long millisPg = DAYS.toMillis(daysPg);
			long millisJava = millisPg + PG_JAVA_EPOCH_DIFF_MILLIS;
			
			return new Date(millisJava);
		}

	}

	static class Encoder implements Type.Codec.Encoder {

		public Class<?> getInputType() {
			return Date.class;
		}

		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			if (val == null) {
				
				buffer.writeInt(-1);
			}
			else {
				
				Date date = (Date) val;
								
				long millisJava = date.getTime();				
				long millisPg = millisJava - PG_JAVA_EPOCH_DIFF_MILLIS;
				int daysPg = (int) MILLISECONDS.toDays(millisPg);
				
				buffer.writeInt(4);
				buffer.writeInt(daysPg);
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
