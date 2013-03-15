package com.impossibl.postgres.system.procs;

import static com.impossibl.postgres.types.Modifiers.LENGTH;

import java.util.HashMap;
import java.util.Map;

import com.impossibl.postgres.types.Modifiers;

public class BitMods extends SimpleProcProvider {

	public BitMods() {
		super(new ModParser(), "bit", "varbit");
	}
	
	static class ModParser implements Modifiers.Parser {

		@Override
		public Map<String, Object> parse(long mod) {
			
			Map<String, Object> mods = new HashMap<String, Object>();
			
			mods.put(LENGTH, (int)mod);
			
			return mods;
		}
		
	}

}
