package com.impossibl.postgres.system.procs;

import static java.lang.reflect.Array.newInstance;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;


/*
 * Array codec
 * 
 */
public class Arrays extends SimpleProcProvider {

	public Arrays() {
		super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "array_", "anyarray_", "oidvector", "intvector");
	}
	
	static class BinDecoder extends BinaryDecoder {
		
		public PrimitiveType getInputPrimitiveType() {
			return PrimitiveType.Array;
		}
		
		public Class<?> getOutputType() {
			return Object[].class;
		}

		public Object decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int length = buffer.readInt();
			
			int readStart = buffer.readerIndex();
			
			Object instance = null;
			
			if(length != -1) {
				
				ArrayType atype = ((ArrayType)type);
				
				//
				//Header
				//
				
				int dimensionCount = buffer.readInt();
				/* boolean hasNulls = */ buffer.readInt() /* == 1 ? true : false */;
				Type elementType = context.getRegistry().loadType(buffer.readInt());
				
				//Each Dimension
				int[] dimensions = new int[dimensionCount];
				int[] lowerBounds = new int[dimensionCount];
				for(int d=0; d < dimensionCount; ++d) {
					
					//Dimension
					dimensions[d] = buffer.readInt();
					
					//Lower bounds
					lowerBounds[d] = buffer.readInt();
				}
				
				if(atype.getElementType().getId() != elementType.getId()) {
					context.refreshType(atype.getId());
				}				

				//
				//Array & Elements
				//

				instance = readArray(buffer, elementType, dimensions, context);
				
				
				if(length != buffer.readerIndex() - readStart) {
					throw new IOException("invalid length");
				}

			}
			
			return instance;
		}
		
		Object readArray(ChannelBuffer buffer, Type type, int[] dims, Context context) throws IOException {
		
			if(dims.length == 0) {
				return readElements(buffer, type, 0, context);
			}
			else if(dims.length == 1) {
				return readElements(buffer, type, dims[0], context);
			}
			else {
				return readSubArray(buffer, type, dims, context);
			}
			
		}
		
		Object readSubArray(ChannelBuffer buffer, Type type, int[] dims, Context context) throws IOException {
			
			Class<?> elementClass = type.unwrap().getJavaType(Collections.<String,Class<?>>emptyMap());
			Object inst = newInstance(elementClass, dims);
			
			int[] subDims = java.util.Arrays.copyOfRange(dims, 1, dims.length);

			for(int c=0; c < dims[0]; ++c) {
				
				Array.set(inst, c, readArray(buffer, type, subDims, context));
				
			}
			
			return inst;
		}
		
		Object readElements(ChannelBuffer buffer, Type type, int len, Context context) throws IOException {
			
			Class<?> elementClass = type.unwrap().getJavaType(Collections.<String,Class<?>>emptyMap());
			Object inst = newInstance(elementClass, len);			
			
			for(int c=0; c < len; ++c) {
				
				Array.set(inst, c, type.getBinaryCodec().decoder.decode(type, buffer, context));
				
			}
			
			return inst;
		}

	}

	static class BinEncoder extends BinaryEncoder {

		public Class<?> getInputType() {
			return Object[].class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return PrimitiveType.Array;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			
			buffer.writeInt(-1);

			if(val != null) {
				
				int writeStart = buffer.writerIndex();
				
				ArrayType atype = ((ArrayType)type);
				Type elementType = atype.getElementType();
				
				//
				//Header
				//
				
				int dimensionCount = getDimensions(val);
				//Dimension count
				buffer.writeInt(dimensionCount);
				//Has nulls
				buffer.writeInt(hasNulls(val) ? 1 : 0);
				//Element type
				buffer.writeInt(elementType.getId());
				
				//each dimension
				Object dim = val;
				for(int d=0; d < dimensionCount; ++d) {
					
					int dimension = 0;
					if(dim != null)
						dimension = Array.getLength(dim);
					
					//Dimension
					buffer.writeInt(dimension);
					
					//Lower bounds
					buffer.writeInt(1);

					if(dimension == 0)
						dim = null;
					else if(dim != null)
						dim = Array.get(dim, 0);
				}
				
				//
				//Array & Elements
				
				writeArray(buffer, elementType, val, context);
				
				//Set length
				buffer.setInt(writeStart-4, buffer.writerIndex() - writeStart);

			}

		}
		
		int getDimensions(Object val) {
			 return 1 + val.getClass().getName().lastIndexOf('[');
		}

		void writeArray(ChannelBuffer buffer, Type type, Object val, Context context) throws IOException {
			
			if(val.getClass().getComponentType().isArray()) {
				
				writeSubArray(buffer, type, val, context);
			}
			else {
				
				writeElements(buffer, type, val, context);
			}
			
		}

		void writeElements(ChannelBuffer buffer, Type type, Object val, Context context) throws IOException {

			int len = Array.getLength(val);
			
			for(int c=0; c < len; ++c) {
				
				type.getBinaryCodec().encoder.encode(type, buffer, Array.get(val, c), context);
			}
			
		}

		void writeSubArray(ChannelBuffer buffer, Type type, Object val, Context context) throws IOException {

			int len = Array.getLength(val);
			
			for(int c=0; c < len; ++c) {
				
				writeArray(buffer, type, Array.get(val, c), context);
			}
			
		}

		boolean hasNulls(Object value) {
			
			for(int c=0, sz = Array.getLength(value); c < sz; ++c) {
				if(Array.get(value, c) == null)
					return true;
			}
			
			return false;
		}

	}

	static class TxtDecoder extends TextDecoder {
		
		public PrimitiveType getInputPrimitiveType() {
			return PrimitiveType.Array;
		}
		
		public Class<?> getOutputType() {
			return Object[].class;
		}

		public Object decode(Type type, CharSequence buffer, Context context) throws IOException {
			
			int length = buffer.length();
			
			Object instance = null;
			
			if(length != 0) {
				
				ArrayType atype = ((ArrayType)type);

				instance = readArray(buffer, atype.getDelimeter(), type.unwrap(), context);
			}
			
			return instance;
		}
		
		Object readArray(CharSequence data, char delim, Type type, Context context) throws IOException {
			
			if(data.length() < 2 || (data.charAt(0) != '{' && data.charAt(data.length()-1) != '}')) {
				return type.getCodec(Format.Text).decoder.decode(type, data, context);
			}
			
			data = data.subSequence(1, data.length()-1);
			
			List<Object> elements = new ArrayList<>();
			StringBuilder elementTxt = new StringBuilder();
			
			boolean string = false;
			int opened = 0;
			int c;
			for(c=0; c < data.length(); ++c) {
				
				char ch = data.charAt(c);
				switch(ch) {
				case '{':
					if(!string)
						opened++;
					else
						elementTxt.append(ch);
					break;
					
				case '}':
					if(!string)
						opened--;
					else
						elementTxt.append(ch);
					break;
					
				case '"':
					string = !string;
					break;
					
				case '\\':
					++c;
					if(c < data.length())
						elementTxt.append(data.charAt(c));
					break;
					
				default:
					
					if(ch == delim && opened == 0 && !string) {
						
						Object element = readArray(elementTxt.toString(), delim, type, context);
						
						elements.add(element);
						
						elementTxt = new StringBuilder();
					}
					else {
						
						elementTxt.append(ch);
					}
					
				}
				
			}
				
			Object finalElement = readArray(elementTxt.toString(), delim, type, context);
			elements.add(finalElement);
			
			return elements.toArray();
		}

	}

	static class TxtEncoder extends TextEncoder {

		public Class<?> getInputType() {
			return Object[].class;
		}

		public PrimitiveType getOutputPrimitiveType() {
			return PrimitiveType.Array;
		}
		
		public void encode(Type type, StringBuilder buffer, Object val, Context context) throws IOException {
			
		}

	}

}
