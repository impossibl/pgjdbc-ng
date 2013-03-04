package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.utils.Factory.createInstance;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;


/*
 * Array codec
 * 
 * TODO: support multi-dimension arrays
 */
public class Arrays extends SimpleProcProvider {

	public Arrays() {
		super(null, null, new Encoder(), new Decoder(), "array_");
	}
	
	static class Decoder implements Type.BinaryIO.Decoder {

		public Object decode(Type type, DataInputStream stream, Context context) throws IOException {

			int lengthGiven = stream.readInt();
			
			long writeStart = stream.getCount();
			
			Object instance = null;
			
			if(lengthGiven != -1) {
				
				ArrayType atype = ((ArrayType)type);
				
				//
				//Header
				//
				
				int dimensionCount = stream.readInt();
				/* boolean hasNulls = */ stream.readInt() /* == 1 ? true : false */;
				Type elementType = Registry.loadType(stream.readInt());
				
				//Each Dimension
				int elementCount = 1;
				int[] dimensions = new int[dimensionCount];
				int[] lowerBounds = new int[dimensionCount];
				for(int d=0; d < dimensionCount; ++d) {
					
					//Dimension
					dimensions[d] = stream.readInt();
					
					//Lower bounds
					lowerBounds[d] = stream.readInt();
					
					
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
					
					Object elementVal = elementType.getBinaryIO().decoder.decode(elementType, stream, context);
					
					Arrays.set(instance, e, elementVal);
				}				
				
			}
			
			if(lengthGiven != (stream.getCount() - writeStart)) {
				throw new IOException("invalid length");
			}

			return instance;
		}

	}

	static class Encoder implements Type.BinaryIO.Encoder {

		public void encode(Type type, DataOutputStream stream, Object val, Context context) throws IOException {
			
			if(val == null) {
				
				stream.writeInt(-1);
			}
			else {
				
				ArrayType atype = ((ArrayType)type);
				Type elementType = atype.getElementType();
				
				//
				//Header
				//
				
				int dimensionCount = Arrays.dimensions(val);
				//Dimension count
				stream.writeInt(dimensionCount);
				//Has nulls
				stream.writeInt(hasNulls(val) ? 1 : 0);
				//Element type
				stream.writeInt(elementType.getId());
				
				//each dimension
				int elementCount = 1;
				for(int d=0; d < dimensionCount; ++d) {
					
					int dimension = Arrays.length(val,d);
					
					//Dimension
					stream.writeInt(dimension);
					
					//Lower bounds
					stream.writeInt(0);
					
					elementCount *= dimension;
				}
				
				//
				//Array & Elements

				for(int e=0; e < elementCount; ++e) {
					
					Object elementVal = Arrays.get(val, e);
					
					elementType.getBinaryIO().encoder.encode(elementType, stream, elementVal, context);
				}

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
