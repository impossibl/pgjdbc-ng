package com.impossibl.postgres.protocol;

import java.util.List;

import com.impossibl.postgres.types.Type;

public interface FunctionCallCommand extends Command {

	String getFunctionName();
	List<Type> getParameterTypes();
	List<Object> getParameterValues();
	Object getResult();

}
