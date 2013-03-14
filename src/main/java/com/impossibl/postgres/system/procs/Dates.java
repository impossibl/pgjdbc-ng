package com.impossibl.postgres.system.procs;

import static org.joda.time.DateTimeZone.UTC;

import java.io.IOException;
import java.sql.Date;

import org.jboss.netty.buffer.ChannelBuffer;
import org.joda.time.DateTime;
import org.joda.time.Days;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;



public class Dates extends SimpleProcProvider {

	private static final DateTime PG_EPOCH = new DateTime(2000,1,1,0,0, UTC);


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
			
			DateTime date = PG_EPOCH.plusDays(daysPg).withZoneRetainFields(context.getTimeZone());
			
			return new Date(date.toDate().getTime());
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
				
				DateTime date = new DateTime((Date) val, context.getTimeZone());
				date = date.withTimeAtStartOfDay().withZoneRetainFields(UTC);
				
				int daysPg = Days.daysBetween(PG_EPOCH, date).getDays();
				
				buffer.writeInt(4);
				buffer.writeInt(daysPg);
			}
			
		}

	}

}
