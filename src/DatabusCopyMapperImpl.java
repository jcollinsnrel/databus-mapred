import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DatabusCopyMapperImpl {
	static final Logger log = LoggerFactory.getLogger(DatabusCopyMapperImpl.class);

	static private IPlayormContext playorm = null;
	static long mapcounter=0;
	static final String KEYSPACE = "databus5";


	public DatabusCopyMapperImpl () {
		String cluster1 = "TestCluster";
		String seeds1 = "sdi-prod-01:9160,sdi-prod-02:9160,sdi-prod-03:9160,sdi-prod-04:9160";
		String port1 = "9160";
		
		String cluster2 = "TestCluster";
		String seeds2 = "sdi-prod-01:9160,sdi-prod-02:9160,sdi-prod-03:9160,sdi-prod-04:9160";
		String port2 = "9160";
		try{
//			System.out.println("the current contextclassloader is "+Thread.currentThread().getContextClassLoader()+" this thread is "+Thread.currentThread());
			Class playormcontextClass = Thread.currentThread().getContextClassLoader().loadClass("PlayormContext");
//			System.out.println("loaded the class for PlayormContext it is "+playormcontextClass);
			Object playormContextObj = playormcontextClass.newInstance();
			Method initmethod = playormcontextClass.getDeclaredMethod("initialize", String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class);
			initmethod.invoke(playormContextObj, KEYSPACE, cluster1, seeds1, port1, KEYSPACE, cluster2, seeds2, port2);
			playorm = (IPlayormContext)playormContextObj;
		}
		catch (Exception e){
			e.printStackTrace();
		}

		//playorm = new PlayormContext();
		//playorm.initialize(KEYSPACE, cluster1, seeds1, port1, KEYSPACE, cluster2, seeds2, port2);
	}

	
	public void map(ByteBuffer keyData, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
    {
		
		try{
//		log.info("about to try to load org.apache.thrift.transport.TTransport");
//		Class c = Thread.currentThread().getContextClassLoader().loadClass("org.apache.thrift.transport.TTransport");
//		Class c2 = Thread.currentThread().getContextClassLoader().loadClass("org.apache.cassandra.thrift.TBinaryProtocol");
//		log.info("loaded org.apache.thrift.transport.TTransport, class is "+c);
//		log.info("loaded org.apache.cassandra.thrift.TBinaryProtocol, class is "+c2);


		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		byte[] key = new byte[keyData.remaining()];
		keyData.get(key);
		
    	mapcounter++;
    	//only do every 10th one for testing:
    	if (mapcounter%10!=1)
    		return;
    	if (mapcounter%1000 == 1) {
    		log.info("called map "+mapcounter+" times.");
    		context.progress();
    	}
    	//super.map(key, columns, context);

    	log.info("performing a map, mapcounter is "+mapcounter);
		if (key.length==0) {
			log.error("GOT A KEY THAT IS SIZE 0!!  WHAT DOES THAT MEAN?");
			return;
		}
		String tableNameIfVirtual = playorm.getTableNameFromKey(key);
		
		if (playorm.sourceTableIsStream(tableNameIfVirtual, key)) {
			transferStream(key, columns, tableNameIfVirtual);
		}
		else {
			transferOrdinary(key, columns, tableNameIfVirtual);
		}
		
		
    }
    

	private void transferOrdinary(byte[] key, SortedMap<ByteBuffer, IColumn> columns, String tableNameIfVirtual) {
		
		String idValue = playorm.getSourceIdColumnValue(tableNameIfVirtual, key);
		String idColName = playorm.getSourceIdColumnName(tableNameIfVirtual);
		log.info("HOW EXCITING!!!  WE GOT A RELATIONAL ROW! for table "+tableNameIfVirtual+" keyColumn = "+idColName+" value="+idValue);
	
		for (IColumn col:columns.values()) {    		
			byte[] namearray = new byte[col.name().remaining()];
    		col.name().get(namearray);
    		byte[] valuearray = new byte[col.value().remaining()];
    		col.value().get(valuearray);
			//Object n = null;
			String colName = playorm.bytesToString(namearray); 
			Object objVal = playorm.sourceConvertFromBytes(tableNameIfVirtual, colName, valuearray);
			

//			try {
//				n = StandardConverters.convertFromBytes(BigDecimal.class, valuearray);
//			}
//			catch (Exception e) {
//				System.err.println(" -- got an exception trying to convert value to BD, it's not a BD!");
//			}
//			try {
//				n = StandardConverters.convertFromBytes(BigInteger.class, valuearray);
//			}
//			catch (Exception e) {
//				System.err.println(" -- got an exception trying to convert value to BI, it's not a BI!");
//			}
			
			log.info("    "+tableNameIfVirtual+", as strings, A (relational) column is "+ colName+", value "+objVal);
		}
		
	}


	private void transferStream(byte[] key, SortedMap<ByteBuffer, IColumn> columns, String tableNameIfVirtual) {
		String time = playorm.getSourceIdColumnValue(tableNameIfVirtual, key);
		String valueAsString = null;
		
		//we are only in here because this is a stream, there is only one column and it's name is "value":
		for (IColumn col:columns.values()) {    		
			byte[] namearray = new byte[col.name().remaining()];
    		col.name().get(namearray);
    		byte[] valuearray = new byte[col.value().remaining()];
    		col.value().get(valuearray);
    		valueAsString = ""+playorm.sourceConvertFromBytes(tableNameIfVirtual, "value", valuearray);
			
    		//String colName = playorm.sourceColumnName(tableNameIfVirtual, namearray);
			//log.info("    As strings, A column is "+ colName+", value "+valueAsString);
		}
		log.info("posting to timeseries table='"+ tableNameIfVirtual +"' key="+time+", value="+valueAsString);

		//TODO!!!!!!  this is just for transfering to the SAME cassandra as a test.  Remove the "Trans" when going to other cassandra instance!
		//postTimeSeries(tableNameIfVirtual+"Trans", time, value, session2);
		
	}

}
