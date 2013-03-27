package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.Modifiers.PRECISION;

import java.util.HashMap;
import java.util.Map;

import com.impossibl.postgres.types.Modifiers;

public class TimeMods extends SimpleProcProvider {

	public TimeMods() {
		super(new ModParser(), "time", "timetz");
	}
	
	static class ModParser implements Modifiers.Parser {

		@Override
		public Map<String, Object> parse(long mod) {
			
			Map<String, Object> mods = new HashMap<String, Object>();
			
			if(mod >= 0)
				mods.put(PRECISION, (int)mod);
			
			return mods;
		}
		
	}

}
