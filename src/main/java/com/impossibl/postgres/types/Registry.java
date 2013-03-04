package com.impossibl.postgres.types;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Logger;

import com.impossibl.postgres.system.procs.Procs;
import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgProc;
import com.impossibl.postgres.system.tables.PgType;
import com.impossibl.postgres.types.Type.BinaryIO;
import com.impossibl.postgres.types.Type.Category;
import com.impossibl.postgres.types.Type.TextIO;

public class Registry {
	
	private static Logger logger = Logger.getLogger(Registry.class.getName());
	
	private static Map<Character, Class<? extends Type>> kindMap;
	static
	{
		kindMap = new HashMap<Character, Class<? extends Type>>();
		kindMap.put('c', CompositeType.class);
		kindMap.put('d', DomainType.class);
		kindMap.put('e', EnumerationType.class);
		kindMap.put('p', PsuedoType.class);
		kindMap.put('r', RangeType.class);
	}

	private static Map<Integer, Type> oidMap = new HashMap<Integer, Type>();
	static
	{
		//Required hardwired type for bootstrapping
		oidMap.put(16, new BaseType(16, "bool", 		(short)1,		(byte)0, Category.Boolean,	',', null, "bool", 		0));
		oidMap.put(17, new BaseType(18, "bytea", 		(short)1,		(byte)0, Category.Numeric, 	',', null, "bytea", 		0));
		oidMap.put(18, new BaseType(18, "char", 		(short)1,		(byte)0, Category.String, 	',', null, "char", 		0));
		oidMap.put(19, new BaseType(19, "name", 		(short)64,	(byte)0, Category.String,		',', null, "name", 		0));
		oidMap.put(21, new BaseType(21, "int2", 		(short)2, 	(byte)0, Category.Numeric,	',', null, "int2", 		0));
		oidMap.put(23, new BaseType(23, "int4", 		(short)4, 	(byte)0, Category.Numeric,	',', null, "int4", 		0));
		oidMap.put(24, new BaseType(24, "regproc", 	(short)4, 	(byte)0, Category.Numeric,	',', null, "regproc", 0));
		oidMap.put(26, new BaseType(26, "oid", 			(short)4,		(byte)0, Category.Numeric,	',', null, "oid",			0));
	}
	
	private static Map<Integer, PgType.Row> pgTypeData = new HashMap<Integer, PgType.Row>();
	private static Map<Integer, Collection<PgAttribute.Row>> pgAttrData = new HashMap<Integer, Collection<PgAttribute.Row>>();
	private static Map<Integer, PgProc.Row> pgProcData = new HashMap<Integer, PgProc.Row>();
	

	public static synchronized Type loadType(int typeId) {
		
		Type type = oidMap.get(typeId);
		if(type == null) {			
			type = loadRaw(typeId);
		}
		
		return type;
	}
	
	public static void update(Collection<PgType.Row> pgTypeRows, Collection<PgAttribute.Row> pgAttrRows, Collection<PgProc.Row> pgProcRows) {
		
		for(PgAttribute.Row pgAttrRow : pgAttrRows) {
			
			Collection<PgAttribute.Row> relRows = pgAttrData.get(pgAttrRow.relationId);
			if(relRows == null) {
				relRows = new HashSet<PgAttribute.Row>();
				pgAttrData.put(pgAttrRow.relationId, relRows);
			}
			
			relRows.add(pgAttrRow);
		}
		
		for(PgProc.Row pgProcRow : pgProcRows) {
			pgProcData.put(pgProcRow.oid, pgProcRow);
		}
		
		for(PgType.Row pgTypeRow : pgTypeRows) {
			pgTypeData.put(pgTypeRow.oid, pgTypeRow);
			oidMap.remove(pgTypeRow.oid);
		}

	}
	
	private static Type loadRaw(int typeId) {
		
		if(typeId==0)
			return null;

		PgType.Row pgType = pgTypeData.get(typeId);
		Collection<PgAttribute.Row> pgAttrs = pgAttrData.get(pgType.relationId);
		
		Type type = loadRaw(pgType, pgAttrs);
		if(type != null) {
			oidMap.put(typeId, type);
		}

		return type;
	}
	
	private static Type loadRaw(PgType.Row pgType, Collection<PgAttribute.Row> pgAttrs) {
		
		Type type;
		
		if(pgType.elementTypeId != 0) {
			
			ArrayType array = new ArrayType();
			array.setElementType(loadType(pgType.elementTypeId));
			
			type = array;
		}
		else {
			
			switch(pgType.discriminator) {
			case 'b':
				type = new BaseType();
				break;
			case 'c':
				type = new CompositeType();
				break;
			case 'd':
				type = new DomainType();
				break;
			case 'e':
				type = new EnumerationType();
				break;
			case 'p':
				type = new PsuedoType();
				break;
			case 'r':
				type = new RangeType();
				break;
			default:
				logger.warning("unknown discriminator (aka 'typtype') found in pg_type table");
				return null;
			}
			
		}
		
		try {
			
			oidMap.put(pgType.oid, type);
		
			type.load(pgType, pgAttrs);
			
		}
		catch(Exception e) {
			
			e.printStackTrace();
			
			oidMap.remove(pgType.oid);
		}

		return type;
	}

	public static BinaryIO loadBinaryIO(int receiveId, int sendId) {
		BinaryIO io = new BinaryIO();
		io.decoder = loadSendProc(sendId);
		io.encoder = loadReceiveProc(receiveId);
		return io;
	}

	public static TextIO loadTextIO(int inputId, int outputId) {
		TextIO io = new TextIO();
		io.encoder = loadInputProc(inputId);
		io.decoder = loadOutputProc(outputId);
		return io;
	}
	
	private static TextIO.Encoder loadInputProc(int inputId) {
		
		String name = findProcName(inputId);
		if(name == null) {
			return null;
		}
		
		//logger.warning("unable to find encoder for input proc: " + name);
		
		return Procs.loadInputProc(name);
	}

	private static TextIO.Decoder loadOutputProc(int outputId) {
		
		String name = findProcName(outputId);
		if(name == null) {
			return null;
		}
		
		//logger.warning("unable to find handler for output proc: " + name);
		
		return Procs.loadOutputProc(name);
	}
	
	private static BinaryIO.Encoder loadReceiveProc(int receiveId) {
		
		String name = findProcName(receiveId);
		if(name == null) {
			return null;
		}
				
		return Procs.loadReceiveProc(name);
	}
	
	private static BinaryIO.Decoder loadSendProc(int sendId) {
		
		String name = findProcName(sendId);
		if(name == null) {
			return null;
		}
		
		return Procs.loadSendProc(name);
	}

	private static String findProcName(int procId) {
		
		PgProc.Row pgProc = pgProcData.get(procId);
		if(pgProc == null)
			return null;
		
		return pgProc.name;
	}

}
