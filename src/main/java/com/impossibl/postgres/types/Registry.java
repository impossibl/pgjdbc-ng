package com.impossibl.postgres.types;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Logger;

import com.impossibl.postgres.system.procs.ProcProvider;
import com.impossibl.postgres.system.procs.Procs;
import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgProc;
import com.impossibl.postgres.system.tables.PgType;
import com.impossibl.postgres.types.Type.BinaryIO;
import com.impossibl.postgres.types.Type.BinaryIO.ReceiveHandler;
import com.impossibl.postgres.types.Type.BinaryIO.SendHandler;
import com.impossibl.postgres.types.Type.TextIO;
import com.impossibl.postgres.types.Type.TextIO.InputHandler;
import com.impossibl.postgres.types.Type.TextIO.OutputHandler;

public class Registry {
	
	private static Logger logger = Logger.getLogger(Registry.class.getName());
	
	private static Map<Character, Class<? extends Type>> kindMap;
	{
		kindMap = new HashMap<Character, Class<? extends Type>>();
		kindMap.put('c', Composite.class);
		kindMap.put('d', Domain.class);
		kindMap.put('e', Enumeration.class);
		kindMap.put('p', Psuedo.class);
		kindMap.put('r', Range.class);
	}

	private static Map<Integer, Type> oidMap = new HashMap<Integer, Type>();
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
		
		switch(pgType.discriminator) {
		case 'b':
			type = new Base();
			break;
		case 'c':
			type = new Composite();
			break;
		case 'd':
			type = new Domain();
			break;
		case 'e':
			type = new Enumeration();
			break;
		case 'p':
			type = new Psuedo();
			break;
		case 'r':
			type = new Range();
			break;
		default:
			logger.warning("unknown discriminator (aka 'typtype') found in pg_type table");
			return null;
		}
		
		try {
			
			oidMap.put(pgType.oid, type);
		
			type.load(pgType, pgAttrs);
			
		}
		catch(Exception e) {
			
			oidMap.remove(pgType.oid);
		}

		return type;
	}

	public static BinaryIO loadBinaryIO(int receiveId, int sendId) {
		BinaryIO io = new BinaryIO();
		io.send = loadSendProc(sendId);
		io.recv = loadReceiveProc(receiveId);
		return io;
	}

	public static TextIO loadTextIO(int inputId, int outputId) {
		TextIO io = new TextIO();
		io.input = loadInputProc(inputId);
		io.output = loadOutputProc(outputId);
		return io;
	}

	private static InputHandler loadInputProc(int inputId) {
		
		String name = findProcName(inputId);
		if(name != null) {

			InputHandler h;
			
			for(ProcProvider pp : Procs.PROVIDERS) {
				if((h = pp.findTextInputHandler(name)) != null)
					return h;
			}

		}
		
		//logger.warning("unable to find handler for input proc: " + name);
		
		return null;
	}

	private static OutputHandler loadOutputProc(int outputId) {
		
		String name = findProcName(outputId);
		if(name != null) {

			OutputHandler h;
			
			for(ProcProvider pp : Procs.PROVIDERS) {
				if((h = pp.findTextOutputHandler(name)) != null)
					return h;
			}

		}
		
		//logger.warning("unable to find handler for output proc: " + name);
		
		return null;
	}

	private static ReceiveHandler loadReceiveProc(int receiveId) {
		
		String name = findProcName(receiveId);
		if(name != null) {

			ReceiveHandler h;
			
			for(ProcProvider pp : Procs.PROVIDERS) {
				if((h = pp.findBinaryReceiveHandler(name)) != null)
					return h;
			}

		}
		
		logger.warning("unable to find handler for receive proc: " + name);
		
		return null;
	}

	private static SendHandler loadSendProc(int sendId) {
		
		String name = findProcName(sendId);
		if(name != null) {

			SendHandler h;
			
			for(ProcProvider pp : Procs.PROVIDERS) {
				if((h = pp.findBinarySendHandler(name)) != null)
					return h;
			}

		}
		
		logger.warning("unable to find handler for send proc: " + name);
		
		return null;
	}

	private static String findProcName(int procId) {
		
		PgProc.Row pgProc = pgProcData.get(procId);
		if(pgProc == null)
			return null;
		
		return pgProc.name;
	}
	
}
