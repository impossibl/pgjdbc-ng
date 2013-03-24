package com.impossibl.postgres.system.procs;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.DomainType;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.Type.Codec;

public class Domains extends SimpleProcProvider {
	
	public Domains() {
		super(new BinEncoder(), null, null, null, "domain_");
	}
	
	public static class BinEncoder implements Codec.Encoder {

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

	public static class TxtEncoder implements Codec.Encoder {

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
			
			baseType.getTextCodec().encoder.encode(baseType, buffer, value, context);
		}
		
	}

}
