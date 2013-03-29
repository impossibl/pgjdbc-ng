package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.PrimitiveType.Record;

import java.io.IOException;
import java.util.Collection;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.data.Record;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.CompositeType.Attribute;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;



public class Records extends SimpleProcProvider {

	public Records() {
		super(null, null, new Encoder(), new Decoder(), "record_");
	}

	static class Decoder implements Type.Codec.Decoder {

		public PrimitiveType getInputPrimitiveType() {
			return Record;
		}
		
		public Class<?> getOutputType() {
			return Record.class;
		}

		public Object decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			CompositeType compType = (CompositeType) type;

			Record record = null;

			int length = buffer.readInt();
			
			if(length != -1) {
				
				long readStart = buffer.readerIndex();
	
				int itemCount = buffer.readInt();

				Object[] attributeVals = new Object[itemCount];
	
				for (int c = 0; c < itemCount; ++c) {
	
					Attribute attribute = compType.getAttribute(c+1);
	
					Type attributeType = context.getRegistry().loadType(buffer.readInt());
	
					if (attributeType.getId() != attribute.type.getId()) {
	
						context.refreshType(attributeType.getId());
					}
	
					Object attributeVal = attributeType.getBinaryCodec().decoder.decode(attributeType, buffer, context);
	
					attributeVals[c] = attributeVal;
				}
	
				if (length != buffer.readerIndex() - readStart) {
					throw new IllegalStateException();
				}
				
				record = new Record(compType, attributeVals);
			}

			return record;
		}

	}

	static class Encoder implements Type.Codec.Encoder {

		public Class<?> getInputType() {
			return Record.class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return Record;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {

			buffer.writeInt(-1);

			if (val != null) {

				int writeStart = buffer.writerIndex();
				
				Record record = (Record) val;
				
				Object[] attributeVals = record.getValues();
				
				CompositeType compType = (CompositeType) type;
				
				Collection<Attribute> attributes = compType.getAttributes();

				buffer.writeInt(attributes.size());

				for(Attribute attribute : attributes) {

					Type attributeType = attribute.type;

					buffer.writeInt(attributeType.getId());

					Object attributeVal = attributeVals[attribute.number-1];

					attributeType.getBinaryCodec().encoder.encode(attributeType, buffer, attributeVal, context);
				}

				//Set length
				buffer.setInt(writeStart-4, buffer.writerIndex() - writeStart);
			}

		}

	}

}
