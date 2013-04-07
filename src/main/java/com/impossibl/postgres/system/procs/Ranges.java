package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.PrimitiveType.Range;
import static com.impossibl.postgres.types.PrimitiveType.Record;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.data.Range;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.RangeType;
import com.impossibl.postgres.types.Type;



public class Ranges extends SimpleProcProvider {

	public Ranges() {
		super(null, null, new Encoder(), new Decoder(), "range_");
	}

	static class Decoder extends BinaryDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return Range;
		}
		
		public Class<?> getOutputType() {
			return Object[].class;
		}

		public Range<?> decode(Type type, ChannelBuffer buffer, Context context) throws IOException {
			
			RangeType rangeType = (RangeType) type;
			Type baseType = rangeType.getBase();
			
			Range<?> instance = null;
			
			int length = buffer.readInt();
			
			if(length != -1) {
				
				Range.Flags flags = new Range.Flags(buffer.readByte());
				Object[] values = new Object[2];
			
				if(flags.hasLowerBound()) {
					
					values[0] = baseType.getBinaryCodec().decoder.decode(baseType, buffer, context);
				}
				
				if(flags.hasUpperBound()) {
					
					values[1] = baseType.getBinaryCodec().decoder.decode(baseType, buffer, context);
				}
				
				instance = new Range<Object>(flags, values);
			}

			return instance;
		}

	}

	static class Encoder extends BinaryEncoder {

		public Class<?> getInputType() {
			return Range.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Record;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

			if (val == null) {

				buffer.writeInt(-1);
			}
			else {
				
				RangeType rangeType = (RangeType) type;
				Type baseType = rangeType.getBase();
				
				Range<?> range = (Range<?>) val;
				
				buffer.writeByte(range.getFlags().getValue());

				if(range.getFlags().hasLowerBound()) {
					
					baseType.getBinaryCodec().encoder.encode(baseType, buffer, range.getLowerBound(), context);
				}

				if(range.getFlags().hasUpperBound()) {
					
					baseType.getBinaryCodec().encoder.encode(baseType, buffer, range.getUpperBound(), context);
				}
				
			}

		}

	}

}
