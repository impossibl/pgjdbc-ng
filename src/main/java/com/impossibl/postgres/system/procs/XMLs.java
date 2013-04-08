package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.types.PrimitiveType;


/*
 * XML codec
 * 
 */
public class XMLs extends SimpleProcProvider {

	public XMLs() {
		super(new TxtEncoder(), new TxtDecoder(), new BinEncoder(), new BinDecoder(), "xml_");
	}
	
	static class BinDecoder extends Bytes.BinDecoder {
		
		public PrimitiveType getInputPrimitiveType() {
			return PrimitiveType.XML;
		}
		
	}

	static class BinEncoder extends Bytes.BinEncoder {

		public PrimitiveType getOutputPrimitiveType() {
			return PrimitiveType.XML;
		}
		
	}

	static class TxtDecoder extends Strings.TxtDecoder {
		
		public PrimitiveType getInputPrimitiveType() {
			return PrimitiveType.XML;
		}
		
	}

	static class TxtEncoder extends Strings.TxtEncoder {

		public PrimitiveType getOutputPrimitiveType() {
			return PrimitiveType.XML;
		}
		
	}

}
