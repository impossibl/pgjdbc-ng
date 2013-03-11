package com.impossibl.postgres.types;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.procs.Procs;
import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgProc;
import com.impossibl.postgres.system.tables.PgType;
import com.impossibl.postgres.types.Type.Category;
import com.impossibl.postgres.types.Type.Codec;


/**
 * Storage and loading for all the known types of a given context.
 * 
 * @author kdubb
 *
 */
public class Registry {

	private static Logger logger = Logger.getLogger(Registry.class.getName());

	private Map<Character, Class<? extends Type>> kindMap;
	private Map<Integer, Type> oidMap;
	private Map<String, Type> nameMap;

	private Map<Integer, PgType.Row> pgTypeData;
	private Map<Integer, Collection<PgAttribute.Row>> pgAttrData;
	private Map<Integer, PgProc.Row> pgProcData;
	private Map<String, PgProc.Row> pgProcNameMap;

	private Context context;

	public Registry(Context context) {

		this.context = context;

		pgTypeData = new TreeMap<>();
		pgAttrData = new TreeMap<>();
		pgProcData = new TreeMap<>();
		pgProcNameMap = new HashMap<>();

		//Maps kinds to their associated type class
		kindMap = new HashMap<>();
		kindMap.put('c', CompositeType.class);
		kindMap.put('d', DomainType.class);
		kindMap.put('e', EnumerationType.class);
		kindMap.put('p', PsuedoType.class);
		kindMap.put('r', RangeType.class);

		// Required initial types for bootstrapping
		oidMap = new HashMap<>();
		oidMap.put(16, new BaseType(16, "bool", 		(short) 1, 	(byte) 0, Category.Boolean, ',', null, "bool", 		0));
		oidMap.put(17, new BaseType(17, "bytea", 		(short) 1, 	(byte) 0, Category.Numeric, ',', null, "bytea", 	0));
		oidMap.put(18, new BaseType(18, "char", 		(short) 1, 	(byte) 0, Category.String, 	',', null, "char", 		0));
		oidMap.put(19, new BaseType(19, "name", 		(short) 64, (byte) 0, Category.String, 	',', null, "name", 		0));
		oidMap.put(21, new BaseType(21, "int2", 		(short) 2, 	(byte) 0, Category.Numeric, ',', null, "int2", 		0));
		oidMap.put(23, new BaseType(23, "int4", 		(short) 4, 	(byte) 0, Category.Numeric, ',', null, "int4", 		0));
		oidMap.put(24, new BaseType(24, "regproc", 	(short) 4, 	(byte) 0, Category.Numeric, ',', null, "regproc", 0));
		oidMap.put(26, new BaseType(26, "oid", 			(short) 4,	(byte) 0, Category.Numeric, ',', null, "oid", 		0));

		nameMap = new HashMap<>();
	}

	/**
	 * Loads a type by its type-id (aka OID)
	 * 
	 * @param typeId The type's id
	 * @return Type object or null, if none found
	 */
	public synchronized Type loadType(int typeId) {

		Type type = oidMap.get(typeId);
		if(type == null) {
			type = loadRaw(typeId);
		}

		return type;
	}

	/**
	 * Looks up a procedures name given it's proc-id (aka OID)
	 * 
	 * @param procId The procedure's id
	 * @return The text name of the procedure or null, if none found
	 */
	public String lookupProcName(int procId) {

		PgProc.Row pgProc = pgProcData.get(procId);
		if(pgProc == null)
			return null;

		return pgProc.name;
	}

	/**
	 * Looks up a procedure id (aka OID) given it's name.
	 * 
	 * @param procName The procedure's name
	 * @return The id of the procedure (aka OID) or null, if none found
	 */
	public int lookupProcId(String procName) {

		PgProc.Row pgProc = pgProcNameMap.get(procName);
		if(pgProc == null)
			return 0;

		return pgProc.oid;
	}

	/**
	 * Updates the type information from the given catalog data. Any
	 * information not specifically mentioned in the given updated
	 * data will be retained and untouched.
	 * 
	 * @param pgTypeRows "pg_type" table rows
	 * @param pgAttrRows "pg_attribute" table rows
	 * @param pgProcRows "pg_proc" table rows
	 */
	public void update(Collection<PgType.Row> pgTypeRows, Collection<PgAttribute.Row> pgAttrRows, Collection<PgProc.Row> pgProcRows) {

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
			pgProcNameMap.put(pgProcRow.name, pgProcRow);
		}

		for(PgType.Row pgTypeRow : pgTypeRows) {
			pgTypeData.put(pgTypeRow.oid, pgTypeRow);
			oidMap.remove(pgTypeRow.oid);
		}

		for(Integer id : pgTypeData.keySet())
			loadType(id);

	}

	/*
	 * Materialize the requested type from the raw catalog data
	 */
	private Type loadRaw(int typeId) {

		if(typeId == 0)
			return null;

		PgType.Row pgType = pgTypeData.get(typeId);
		Collection<PgAttribute.Row> pgAttrs = pgAttrData.get(pgType.relationId);

		Type type = loadRaw(pgType, pgAttrs);
		if(type != null) {
			oidMap.put(typeId, type);
			nameMap.put(type.getName(), type);
		}

		return type;
	}

	/*
	 * Materialize a type from the given "pg_type" and "pg_attribute" data
	 */
	private Type loadRaw(PgType.Row pgType, Collection<PgAttribute.Row> pgAttrs) {

		Type type;

		if(pgType.elementTypeId != 0) {

			ArrayType array = new ArrayType();
			array.setElementType(loadType(pgType.elementTypeId));

			type = array;
		}
		else {

			switch(pgType.discriminator.charAt(0)) {
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

			type.load(pgType, pgAttrs, this);

		}
		catch(Exception e) {

			e.printStackTrace();

			oidMap.remove(pgType.oid);
		}

		return type;
	}

	/**
	 * Loads a matching Codec given the proc-id of its encoder and decoder
	 * 
	 * @param encoderId	proc-id of the encoder
	 * @param decoderId proc-id of the decoder
	 * @return A matching Codec instance
	 */
	public Codec loadCodec(int encoderId, int decoderId) {
		Codec io = new Codec();
		io.decoder = loadDecoderProc(decoderId);
		io.encoder = loadEncoderProc(encoderId);
		return io;
	}

	/*
	 * Loads a matching encoder given its proc-id
	 */
	private Codec.Encoder loadEncoderProc(int procId) {

		String name = lookupProcName(procId);
		if(name == null) {
			return null;
		}

		return Procs.loadEncoderProc(name, context);
	}

	/*
	 * Loads a matching decoder given its proc-id
	 */
	private Codec.Decoder loadDecoderProc(int procId) {

		String name = lookupProcName(procId);
		if(name == null) {
			return null;
		}

		return Procs.loadDecoderProc(name, context);
	}

}
