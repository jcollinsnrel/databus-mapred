import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.TypedRow;



public class PlayormContext {
	
	static final Logger log = LoggerFactory.getLogger(PlayormContext.class);

	private NoSqlEntityManager sourceMgr;
    private NoSqlEntityManager destMgr;
    
    public PlayormContext(Object keyspace, Object cluster1, Object seeds1, Object port1, Object keyspace2, Object cluster2, Object seeds2, Object port2) {
    	Map<String, Object> props = new HashMap<String, Object>();
		props.put(Bootstrap.TYPE, "cassandra");
		props.put(Bootstrap.CASSANDRA_KEYSPACE, keyspace);
		props.put(Bootstrap.CASSANDRA_CLUSTERNAME, cluster1);
		props.put(Bootstrap.CASSANDRA_SEEDS, seeds1);
		props.put(Bootstrap.CASSANDRA_THRIFT_PORT, port1);
		props.put(Bootstrap.AUTO_CREATE_KEY, "create");
		//props.put(Bootstrap.LIST_OF_EXTRA_CLASSES_TO_SCAN_KEY, classes);

		NoSqlEntityManagerFactory factory1 = Bootstrap.create(props, Thread.currentThread().getContextClassLoader());  //that 'null' is a classloader that supposed to come from play...  does null work?
		sourceMgr = factory1.createEntityManager();
		
		Map<String, Object> props2 = new HashMap<String, Object>();
		props2.put(Bootstrap.TYPE, "cassandra");
		props2.put(Bootstrap.CASSANDRA_KEYSPACE, keyspace2);
		props2.put(Bootstrap.CASSANDRA_CLUSTERNAME, cluster2);
		props2.put(Bootstrap.CASSANDRA_SEEDS, seeds2);
		props2.put(Bootstrap.CASSANDRA_THRIFT_PORT, port2);
		props2.put(Bootstrap.AUTO_CREATE_KEY, "create");
		//props2.put(Bootstrap.LIST_OF_EXTRA_CLASSES_TO_SCAN_KEY, classes);

		NoSqlEntityManagerFactory factory2 = Bootstrap.create(props2, Thread.currentThread().getContextClassLoader());  //that 'null' is a classloader that supposed to come from play...  does null work?
		destMgr = factory2.createEntityManager();
    }
    
    public String getTableNameFromKey(byte[] key) {
    	return DboColumnIdMeta.fetchTableNameIfVirtual(key);
    }
    
    public boolean sourceTableIsStream(String tableNameIfVirtual, byte[] key) {
    	DboTableMeta meta = sourceMgr.find(DboTableMeta.class, tableNameIfVirtual);
		DboColumnMeta[] allColumns = meta.getAllColumns().toArray(new DboColumnMeta[]{});

		String idColumnName = meta.getIdColumnMeta().getColumnName();
    	if (allColumns.length==1 && "value".equals(allColumns[0].getColumnName()) && "time".equals(idColumnName)) 
    		return true;
    	return false;
	}
    
    public void postTimeSeries(String tableNameIfVirtual, Object pkValue, Object value, NoSqlTypedSession typedSession) {

    	DboTableMeta table = destMgr.find(DboTableMeta.class, tableNameIfVirtual);
		if (log.isInfoEnabled())
			log.info("writing to Timeseries, table name!!!!!!! = '" + table.getColumnFamily() + "'");
		String cf = table.getColumnFamily();
		
		DboColumnMeta idColumnMeta = table.getIdColumnMeta();
		//rowKey better be BigInteger
		if (log.isInfoEnabled())
			log.info("writing to '" + table.getColumnFamily() + "', pk is '" + pkValue + "'");
		Object timeStamp = convertToStorage(idColumnMeta, pkValue);
		byte[] colKey = idColumnMeta.convertToStorage2(timeStamp);
		BigInteger time = (BigInteger) timeStamp;
		long longTime = time.longValue();
		//find the partition
		Long partitionSize = table.getTimeSeriesPartionSize();
		long partitionKey = (longTime / partitionSize) * partitionSize;

		TypedRow row = typedSession.createTypedRow(table.getColumnFamily());
		row.setRowKey(new BigInteger(""+partitionKey));	
		
		Collection<DboColumnMeta> cols = table.getAllColumns();

		DboColumnMeta col = cols.iterator().next();
		if(value == null) {
			if (log.isWarnEnabled())
				log.warn("The table you are inserting requires column='"+col.getColumnName()+"' to be set and null was passed in");
			throw new RuntimeException("The table you are inserting requires column='"+col.getColumnName()+"' to be set and null is passed in");
		}
		
		Object newValue = convertToStorage(col, value);
		byte[] val = col.convertToStorage2(newValue);
		row.addColumn(colKey, val, null);

		//This method also indexes according to the meta data as well
		typedSession.put(cf, row);
	}
    
    public static Object convertToStorage(DboColumnMeta col, Object someVal) {
		try {
			if(someVal == null)
				return null;
			else if("null".equals(someVal))
				return null; //a fix for when they pass us "null" instead of null
			
			String val = ""+someVal;
			System.err.println("VALUE is "+val);
			if(val.length() == 0)
				val = null;
			return col.convertStringToType(val);
		} catch(Exception e) { 
			//Why javassist library throws a checked exception, I don't know as we can't catch a checked exception here
			if(e instanceof InvocationTargetException &&
					e.getCause() instanceof NumberFormatException) {
				if (log.isWarnEnabled())
	        		log.warn("Cannot convert value="+someVal+" for column="+col.getColumnName()+" table="+col.getOwner().getRealColumnFamily()+" as it needs to be type="+col.getClassType(), e.getCause());
			}
			throw new RuntimeException(e);
		}
	}
    
    public String getSourceIdColumnValue(String tableNameIfVirtual, byte[] key) {
    	DboTableMeta meta = sourceMgr.find(DboTableMeta.class, tableNameIfVirtual);
    	byte[] nonvirtkey = meta.getIdColumnMeta().unformVirtRowKey(key);
		return ""+meta.getIdColumnMeta().convertFromStorage2(nonvirtkey);
    }
    
	public String getSourceIdColumnName(String tableNameIfVirtual) {
		DboTableMeta meta = sourceMgr.find(DboTableMeta.class, tableNameIfVirtual);
		return meta.getIdColumnMeta().getColumnName();
	}

	public Number sourceConvertFromBytes(String tableNameIfVirtual, String columnName,
			byte[] valuearray) {
		DboTableMeta meta = sourceMgr.find(DboTableMeta.class, tableNameIfVirtual);
		DboColumnMeta columnMeta = meta.getColumnMeta(columnName);
		return (Number)columnMeta.convertFromStorage2(valuearray);
	}
	
	public String bytesToString(byte[] namearray) {
		return StandardConverters.convertFromBytes(String.class, namearray);
	}

}
