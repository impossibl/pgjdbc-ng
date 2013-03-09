package com.impossibl.postgres.system.procs;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.sql.Time;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;



public class TimesWithTZ extends SettingSelectProcProvider {

	public TimesWithTZ() {
		super("datetimes.binary.class", Integer.class,
				null, null, new BinIntegerEncoder(), new BinIntegerDecoder(),
				null, null, null, null,
				"timetz_");
	}

	static class BinIntegerDecoder implements Type.BinaryIO.Decoder {

		public Time decode(Type type, DataInputStream stream, Context context) throws IOException {

			int length = stream.readInt();
			if (length == -1) {
				return null;
			}
			else if (length != 12) {
				throw new IOException("invalid length");
			}

			long baseMicros = stream.readLong();
			int tzOffsetSecs = stream.readInt();
			
			long micros = baseMicros+ SECONDS.toMicros(tzOffsetSecs);
			
			long millis = MICROSECONDS.toMillis(micros);
			
			return new Time(millis);
		}

	}

	static class BinIntegerEncoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			if (val == null) {

				stream.writeInt(-1);
			}
			else {
				
				Time time = (Time) val;
				
				long millis = time.getTime();
				
				long baseMicros = MILLISECONDS.toMicros(millis);
				int tzOffsetSecs = (int)MILLISECONDS.toSeconds(context.getTimeZone().getRawOffset());
				
				stream.writeInt(12);
				stream.writeLong(baseMicros);
				stream.writeInt(tzOffsetSecs);
			}

		}

	}
	
}
