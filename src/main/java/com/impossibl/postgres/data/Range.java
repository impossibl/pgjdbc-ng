package com.impossibl.postgres.data;

public class Range<T> {
	
	public static class Flags {
		
		byte value;
	
		private static final byte RANGE_EMPTY 	= 0x01;	/* range is empty */
		private static final byte RANGE_LB_INC 	= 0x02;	/* lower bound is inclusive */
		private static final byte RANGE_UB_INC 	= 0x04;	/* upper bound is inclusive */
		private static final byte RANGE_LB_INF 	= 0x08;	/* lower bound is -infinity */
		private static final byte RANGE_UB_INF 	= 0x10;	/* upper bound is +infinity */
		
		public Flags(byte value) {
			super();
			this.value = value;
		}
		
		public byte getValue() {
			return value;
		}

		public boolean isEmpty() {
			return (value & RANGE_EMPTY) != 0;
		}
		
		public boolean hasLowerBound() {
			return (value & RANGE_LB_INC) != 0;
		}
		
		public boolean isLowerBoundInfinity() {
			return (value & RANGE_LB_INF) != 0;
		}

		public boolean hasUpperBound() {
			return (value & RANGE_UB_INC) != 0;
		}

		public boolean isUpperBoundInfinity() {
			return (value & RANGE_UB_INF) != 0;
		}

	}

	Flags flags;
	Object[] values;
	
	public Range(Flags flags, Object[] values) {
		this.flags = flags;
		this.values = values.clone();
	}
	
	public Flags getFlags() {
		return flags;
	}
	
	public boolean isEmpty() {
		return flags.isEmpty();
	}
	
	public boolean hasLowerBound() {
		return flags.hasLowerBound();
	}
	
	@SuppressWarnings("unchecked")
	public T getLowerBound() {
		return (T) values[0];
	}
	
	public boolean isLowerBoundInfinity() {
		return flags.isLowerBoundInfinity();
	}

	public boolean hasUpperBound() {
		return flags.hasUpperBound();
	}

	@SuppressWarnings("unchecked")
	public T getUpperBound() {
		return (T) values[1];
	}
	
	public boolean isUpperBoundInfinity() {
		return flags.isUpperBoundInfinity();
	}

}
