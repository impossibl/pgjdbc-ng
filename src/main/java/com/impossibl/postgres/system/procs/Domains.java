package com.impossibl.postgres.system.procs;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.DomainType;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;

public class Domains extends SimpleProcProvider {
	
	public Domains() {
		super(new TxtEncoder(), null, new BinEncoder(), null, "domain_");
	}
	
	public static class BinEncoder extends BinaryEncoder {

		@Override
		public Class<?> getInputType() {
			return null;	//any
		}

		@Override
		public PrimitiveType getOutputPrimitiveType() {
			return null;	//any
		}

		@Override
		public void encode(Type type, ChannelBuffer buffer, Object value, Context context) throws IOException {

			DomainType domainType = (DomainType) type;
			Type baseType = domainType.getBase();
			
			baseType.getBinaryCodec().encoder.encode(baseType, buffer, value, context);
		}
		
	}

	public static class TxtEncoder extends TextEncoder {

		@Override
		public Class<?> getInputType() {
			return null;	//any
		}

		@Override
		public PrimitiveType getOutputPrimitiveType() {
			return null;	//any
		}

		@Override
		public void encode(Type type, StringBuilder buffer, Object value, Context context) throws IOException {

			DomainType domainType = (DomainType) type;
			Type baseType = domainType.getBase();
			
			baseType.getTextCodec().encoder.encode(baseType, buffer, value, context);
		}
		
	}

}
