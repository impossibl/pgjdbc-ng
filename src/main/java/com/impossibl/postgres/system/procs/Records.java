package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.PrimitiveType.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.data.Record;
import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.CompositeType.Attribute;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;



public class Records extends SimpleProcProvider {

	public Records() {
		super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "record_");
	}

	static class BinDecoder extends BinaryDecoder {

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

	static class BinEncoder extends BinaryEncoder {

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

	static class TxtDecoder extends TextDecoder {
		
		public PrimitiveType getInputPrimitiveType() {
			return PrimitiveType.Record;
		}
		
		public Class<?> getOutputType() {
			return Object[].class;
		}

		public Object decode(Type type, CharSequence buffer, Context context) throws IOException {
			
			int length = buffer.length();
			
			Object instance = null;
			
			if(length != 0) {
				
				instance = readComposite(buffer, type.getDelimeter(), (CompositeType) type, context);
			}
			
			return instance;
		}
		
		Object readValue(CharSequence data, Type type, Context context) throws IOException {
			
			if(type instanceof CompositeType) {
				
				
				return readComposite(data, type.getDelimeter(), (CompositeType) type, context);
			}
			else {
								
				return type.getCodec(Format.Text).decoder.decode(type, data, context);
			}

		}
		
		Object readComposite(CharSequence data, char delim, CompositeType type, Context context) throws IOException {
			
			if(data.length() < 2 || (data.charAt(0) != '(' && data.charAt(data.length()-1) != ')')) {
				return null;
			}
			
			data = data.subSequence(1, data.length()-1);
			
			List<Object> elements = new ArrayList<>();
			StringBuilder elementTxt = new StringBuilder();
			int elementIdx = 1;
			
			boolean string = false;
			int opened = 0;
			int c;
			for(c=0; c < data.length(); ++c) {
				
				char ch = data.charAt(c);
				switch(ch) {
				case '(':
					if(!string)
						opened++;
					else
						elementTxt.append(ch);
					break;
					
				case ')':
					if(!string)
						opened--;
					else
						elementTxt.append(ch);
					break;
					
				case '"':
					if(c < data.length() && data.charAt(c+1) == '"') {
						elementTxt.append('"');
						c++;
					}
					else {
						string = !string;
					}
					break;
					
				default:
					
					if(ch == delim && opened == 0 && !string) {
						
						Object element = readValue(elementTxt.toString(), type.getAttribute(elementIdx).type, context);
						
						elements.add(element);
						
						elementTxt = new StringBuilder();
						elementIdx++;
					}
					else {
						
						elementTxt.append(ch);
					}
					
				}
				
			}
				
			Object finalElement = readValue(elementTxt.toString(), type.getAttribute(elementIdx).type, context);
			elements.add(finalElement);
			
			return elements.toArray();
		}

	}

	static class TxtEncoder extends TextEncoder {

		public Class<?> getInputType() {
			return Object[].class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return PrimitiveType.Record;
		}
		
		public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {
			
		}

	}

}
