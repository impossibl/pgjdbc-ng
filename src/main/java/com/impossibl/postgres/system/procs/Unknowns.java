package com.impossibl.postgres.system.procs;

import java.util.Collections;
import java.util.Map;

import com.impossibl.postgres.types.Modifiers;
import com.impossibl.postgres.types.PrimitiveType;



public class Unknowns {

	public static class BinDecoder extends Bytes.BinDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return PrimitiveType.Unknown;
		}

	}

	public static class BinEncoder extends Bytes.BinEncoder {

		public PrimitiveType getOutputPrimitiveType() {
			return PrimitiveType.Unknown;
		}

	}

	public static class TxtDecoder extends Strings.TxtDecoder {

		public PrimitiveType getInputPrimitiveType() {
			return PrimitiveType.Unknown;
		}

	}

	public static class TxtEncoder extends Strings.TxtEncoder {

		public PrimitiveType getOutputPrimitiveType() {
			return PrimitiveType.Unknown;
		}

	}

	public static class ModParser implements Modifiers.Parser {

		@Override
		public Map<String, Object> parse(long mod) {
			return Collections.<String, Object> emptyMap();
		}

	}

}
