package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.PrimitiveType.Interval;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.data.Interval;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;


public class Intervals extends SimpleProcProvider {

	public Intervals() {
		super(null, null, new Encoder(), new Decoder(), "interval_");
	}

	static class Decoder implements Type.Codec.Decoder {

		public PrimitiveType getInputPrimitiveType() {
			return Interval;
		}
		
		public Class<?> getOutputType() {
			return Interval.class;
		}

		public Interval decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			if (length == -1) {
				return null;
			}
			else if (length != 16) {
				throw new IOException("invalid length");
			}

			long timeMicros = buffer.readLong();
			int days = buffer.readInt();
			int months = buffer.readInt();
			
			return new Interval(months, days, timeMicros);
		}

	}

	static class Encoder implements Type.Codec.Encoder {

		public Class<?> getInputType() {
			return Interval.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Interval;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

			if (val == null) {
				
				buffer.writeInt(-1);
			}
			else {
				
				Interval interval = (Interval) val;
				
				buffer.writeInt(16);
				buffer.writeLong(interval.getRawTime());
				buffer.writeInt(interval.getRawDays());
				buffer.writeInt(interval.getRawMonths());
			}
			
		}

	}

}
