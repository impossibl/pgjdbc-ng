package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.utils.Factory.createInstance;

import java.io.IOException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.Type;


/*
 * Array codec
 * 
 * TODO: support multi-dimension arrays
 */
public class Arrays extends SimpleProcProvider {

	public Arrays() {
		super(null, null, new Encoder(), new Decoder(), "array_");
	}
	
	static class Decoder implements Type.Codec.Decoder {
		
		public int getInputSQLType() {
			return Types.ARRAY;
		}
		
		public Class<?> getOutputType() {
			return Object[].class;
		}

		public Object decode(Type type, ChannelBuffer buffer, Context context) throws IOException {

			int lengthGiven = buffer.readInt();
			
			int readStart = buffer.readerIndex();
			
			Object instance = null;
			
			if(lengthGiven != -1) {
				
				ArrayType atype = ((ArrayType)type);
				
				//
				//Header
				//
				
				int dimensionCount = buffer.readInt();
				/* boolean hasNulls = */ buffer.readInt() /* == 1 ? true : false */;
				Type elementType = context.getRegistry().loadType(buffer.readInt());
				
				//Each Dimension
				int elementCount = 1;
				int[] dimensions = new int[dimensionCount];
				int[] lowerBounds = new int[dimensionCount];
				for(int d=0; d < dimensionCount; ++d) {
					
					//Dimension
					dimensions[d] = buffer.readInt();
					
					//Lower bounds
					lowerBounds[d] = buffer.readInt();
					
					
					elementCount *= dimensions[d];
				}
				
				if(atype.getElementType().getId() != elementType.getId()) {
					context.refreshType(atype.getId());
				}				

				//
				//Array & Elements
				//
				
				instance = createInstance(context.lookupInstanceType(type), elementCount);
				
				for(int e=0; e < elementCount; ++e) {
					
					Object elementVal = elementType.getBinaryCodec().decoder.decode(elementType, buffer, context);
					
					Arrays.set(instance, e, elementVal);
				}				
				
			}
			
			if(lengthGiven != buffer.readerIndex() - readStart) {
				throw new IOException("invalid length");
			}

			return instance;
		}

	}

	static class Encoder implements Type.Codec.Encoder {

		public Class<?> getInputType() {
			return Object[].class;
		}

		public int getOutputSQLType() {
			return Types.ARRAY;
		}
		
		public void encode(Type type, ChannelBuffer buffer, Object val, Context context) throws IOException {
			
			if(val == null) {
				
				buffer.writeInt(-1);
			}
			else {
				
				buffer.writeInt(-1);
				
				int writeStart = buffer.writerIndex();
				
				ArrayType atype = ((ArrayType)type);
				Type elementType = atype.getElementType();
				
				//
				//Header
				//
				
				int dimensionCount = Arrays.dimensions(val);
				//Dimension count
				buffer.writeInt(dimensionCount);
				//Has nulls
				buffer.writeInt(hasNulls(val) ? 1 : 0);
				//Element type
				buffer.writeInt(elementType.getId());
				
				//each dimension
				int elementCount = 1;
				for(int d=0; d < dimensionCount; ++d) {
					
					int dimension = Arrays.length(val,d);
					
					//Dimension
					buffer.writeInt(dimension);
					
					//Lower bounds
					buffer.writeInt(0);
					
					elementCount *= dimension;
				}
				
				//
				//Array & Elements

				for(int e=0; e < elementCount; ++e) {
					
					Object elementVal = Arrays.get(val, e);
					
					elementType.getBinaryCodec().encoder.encode(elementType, buffer, elementVal, context);
				}
				
				//Set length
				buffer.setInt(writeStart-4, buffer.writerIndex() - writeStart);

			}

		}

	}

	public static int dimensions(Object val) {
		return 1;
	}

	@SuppressWarnings("rawtypes")
	public static int length(Object val, int dimension) throws IOException {
		
		if(dimension != 0) {
			throw new IllegalStateException("multi-dimensional arrays not supported");
		}
		
		if(val.getClass().isArray()) {
			return java.lang.reflect.Array.getLength(val);
		}
		else if(val instanceof List) {
			return ((List)val).size();
		}
		else if(val instanceof Map) {
			return ((Map)val).size();
		}
		else {
			throw new IOException("unsupported collection type");
		}
	}

	@SuppressWarnings("rawtypes")
	public static Object get(Object val, int idx) throws IOException {

		if(val.getClass().isArray()) {
			return java.lang.reflect.Array.get(val, idx);
		}
		else if(val instanceof List) {
			return ((List)val).get(idx);
		}
		else if(val instanceof Map) {
			return ((Map)val).get(idx);
		}
		else {
			throw new IOException("unsupported collection type");
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void set(Object val, int idx, Object elementVal) throws IOException {

		if(val.getClass().isArray()) {
			java.lang.reflect.Array.set(val, idx, elementVal);
		}
		else if(val instanceof List) {
			((List)val).add(elementVal);
		}
		else if(val instanceof Map) {
			((Map)val).put(idx, elementVal);
		}
		else {
			throw new IOException("unsupported collection type");
		}
	}

	public static boolean hasNulls(Object value) {
		
		return java.util.Arrays.asList((Object[])value).contains(null);
	}

	public static boolean is(Object val) {
		return val.getClass().isArray() || val instanceof List;
	}
	
}
