package com.impossibl.postgres.system.procs;

public abstract class BaseProcProvider implements ProcProvider {

	
	String[] baseNames;
	

	public BaseProcProvider(String[] baseNames) {
		super();
		this.baseNames = baseNames;
	}

	protected boolean hasName(String name, String suffix) {
		
		for(String baseName : baseNames) {
			if(name.equals(baseName+suffix))
				return true;
		}
		
		return false;
	}

}
