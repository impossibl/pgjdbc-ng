package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.Modifiers.PRECISION;
import static com.impossibl.postgres.types.Modifiers.SCALE;

import java.util.HashMap;
import java.util.Map;

import com.impossibl.postgres.types.Modifiers;

public class NumericMods extends SimpleProcProvider {

	public NumericMods() {
		super(new ModParser(), "numeric");
	}
	
	static class ModParser implements Modifiers.Parser {

		@Override
		public Map<String, Object> parse(long mod) {
			
			Map<String, Object> mods = new HashMap<String, Object>();
			
			if(mod > 4) {
				mods.put(PRECISION, (int)((mod-4) >> 16) & 0xffff);
				mods.put(SCALE, 		(int)((mod-4) >>  0) & 0xffff);
			}
			else {
				mods.put(PRECISION, (int)0);
				mods.put(SCALE, 		(int)0);				
			}
			
			return mods;
		}
		
	}

}
