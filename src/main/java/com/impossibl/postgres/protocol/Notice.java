package com.impossibl.postgres.protocol;

public class Notice {
	
	public static final String SUCCESS_CLASS 										= "00";
	public static final String WARNING_CLASS 										= "01";
	public static final String NO_DATA_CLASS 										= "02";
	public static final String STATEMENT_INCOMPLETE_CLASS				= "03";
	public static final String CONNECTION_EXC_CLASS							= "08";
	public static final String TRIGGERED_ACTION_EXC_CLASS				= "09";
	public static final String FEATURE_NOT_SUPPORTED_CLASS			= "0A";
	public static final String INVALID_TXN_INIT_CLASS						= "0B";
	public static final String LOCATOR_EXC_CLASS								= "0F";
	public static final String INVALID_GRANTOR_CLASS						= "0L";
	public static final String INVALID_ROLE_SPEC_CLASS					= "0P";
	public static final String DIAGNOSTICS_EXC_CLASS						= "0Z";
	public static final String CASE_NOT_FOUNC_CLASS							= "20";
	public static final String CARDINALITY_VIOL_CLASS						= "21";
	public static final String DATA_EXC_CLASS										= "22";
	public static final String INTEGRITY_CONST_VIOL_CLASS				= "23";
	public static final String INVALID_CURSOR_STATE_CLASS				= "24";
	public static final String INVALID_TXN_STATE_CLASS					= "25";
	public static final String INVALID_STATEMENT_NAME_CLASS 		= "26";
	public static final String TRIGGERED_DATA_CHANGE_VIOL_CLASS = "27";
	public static final String INVALID_AUTHZN_SPEC_CLASS				= "28";
	public static final String DEP_DESCRIPTOR_EXISTS_CLASS			= "2B";
	public static final String INVALID_TXN_TERM_CLASS						= "2D";
	public static final String SQL_ROUTINE_EXC_CLASS						= "2F";
	public static final String INVALID_CURSOR_NAME_CLASS				= "34";
	public static final String EXT_ROUTINE_EXC_CLASS						= "38";
	public static final String EXT_ROUTINE_INV_EXC_CLASS				= "39";
	public static final String SAVEPOINT_EXC_CLASS							= "3B";
	public static final String INVALID_CATALOG_NAME_CLASS				= "3D";
	public static final String INVALID_SCHEMA_NAME_CLASS				= "3F";
	public static final String TRANSACTION_ROLLBACK_CLASS				= "40";
	public static final String SYNTAX_OR_ACCESS_ERROR_CLASS			= "42";
	public static final String CHECK_VIOL_CLASS									= "44";
	public static final String INSUFFICIENT_RESOURCES_CLASS			= "53";
	public static final String PROGRAM_LIMIT_EXCEEDED_CLASS			= "54";
	public static final String OBJECT_PREREQ_STATE__CLASS				= "55";
	public static final String OPERATOR_INTERVENTION_CLASS			= "57";
	public static final String SYSTEM_ERROR_CLASS								= "58";
	public static final String CONFIG_ERROR_CLASS								= "F0";
	public static final String FOREIGN_DATA_EXC_CLASS						= "HV";
	public static final String PL_PGSQL_CLASS										= "P0";
	public static final String INTERNAL_ERROR_CLASS							= "XX";
	
	
	public String severity;
	public String code;
	public String message;
	public String detail;
	public String hint;
	public String position;
	public String where;
	public String routine;
	public String file;
	public String line;
	
	
	public Notice() {
	}
	
	public boolean isSuccess() {
		return code != null && code.startsWith(SUCCESS_CLASS);
	}
	
	public boolean isWarning() {
		return code != null && 
				(code.startsWith(WARNING_CLASS) || code.startsWith(NO_DATA_CLASS)); 
	}
	
	public boolean isError() {
		return code != null && !isSuccess() && !isWarning();
	}
	
}
