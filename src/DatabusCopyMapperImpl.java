import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fromporm.conv.StandardConverters;


public class DatabusCopyMapperImpl {
	static final Logger log = LoggerFactory.getLogger(DatabusCopyMapperImpl.class);

	static private IPlayormContext playorm = null;
	static long mapcounter=0;
	static final String KEYSPACE = "databus5";
	static final String KEYSPACE2 = "databus";
	
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text("count");
    private Text written = new Text("written");
    
    private String[] streamColNames=new String[]{"time", "value"};


	public DatabusCopyMapperImpl () {
		//String cluster1 = "QACluster";
		String cluster1 = "DatabusCluster";
		//String seeds1 = "sdi-prod-01:9160,sdi-prod-02:9160,sdi-prod-03:9160,sdi-prod-04:9160";
		String seeds1 = "a1.bigde.nrel.gov:9160,a2.bigde.nrel.gov:9160,a3.bigde.nrel.gov:9160,a4.bigde.nrel.gov:9160,a5.bigde.nrel.gov:9160,a6.bigde.nrel.gov:9160,a7.bigde.nrel.gov:9160,a8.bigde.nrel.gov:9160,a9.bigde.nrel.gov:9160,a10.bigde.nrel.gov:9160,a11.bigde.nrel.gov:9160,a12.bigde.nrel.gov:9160";
		String port1 = "9160";
		
		//String cluster2 = "QAClusterB";
		String cluster2 = "DatabusClusterB";
		//String seeds2 = "sdi-prod-01:9158,sdi-prod-02:9158,sdi-prod-03:9158,sdi-prod-04:9158";
		String seeds2 = "a1.bigde.nrel.gov:9158,a2.bigde.nrel.gov:9158,a3.bigde.nrel.gov:9158,a4.bigde.nrel.gov:9158,a5.bigde.nrel.gov:9158,a6.bigde.nrel.gov:9158,a7.bigde.nrel.gov:9158,a8.bigde.nrel.gov:9158,a9.bigde.nrel.gov:9158,a10.bigde.nrel.gov:9158,a11.bigde.nrel.gov:9158,a12.bigde.nrel.gov:9158";
		String port2 = "9158";
		try{
//			System.out.println("the current contextclassloader is "+Thread.currentThread().getContextClassLoader()+" this thread is "+Thread.currentThread());
			Class playormcontextClass = Thread.currentThread().getContextClassLoader().loadClass("PlayormContext");
//			System.out.println("loaded the class for PlayormContext it is "+playormcontextClass);
			Object playormContextObj = playormcontextClass.newInstance();
			Method initmethod = playormcontextClass.getDeclaredMethod("initialize", String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class);
			initmethod.invoke(playormContextObj, KEYSPACE, cluster1, seeds1, port1, KEYSPACE2, cluster2, seeds2, port2);
			playorm = (IPlayormContext)playormContextObj;
		}
		catch (Exception e){
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}

	
	public void map(ByteBuffer keyData, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
    {
		
		byte[] key = new byte[keyData.remaining()];
		keyData.get(key);
		if (key.length==0) {
			log.error("GOT A KEY THAT IS SIZE 0!!  WHAT DOES THAT MEAN?");
			return;
		}
    	mapcounter++;
    	String tableNameIfVirtual = playorm.getTableNameFromKey(key);
    	
    	if (mapcounter%10000 == 1) {
    		log.info("called map "+mapcounter+" times.");
    		//when this was writing to context instead of doing the copy directly in the map phase it was 
    		//timing out, this prevents that.  Now that we are copying the data in the map phase it is not needed:
    		//context.progress();
    	}
		
		if (playorm.sourceTableIsStream(tableNameIfVirtual, key)) {
			transferStream(key, columns, tableNameIfVirtual, context);
		}
		else {
			transferOrdinary(key, columns, tableNameIfVirtual, context);
		}
		
		
    }
    

	private void transferOrdinary(byte[] key, SortedMap<ByteBuffer, IColumn> columns, String tableNameIfVirtual, Context context) throws IOException, InterruptedException {
		
		String idValue = playorm.getSourceIdColumnValue(tableNameIfVirtual, key);
		String idColName = playorm.getSourceIdColumnName(tableNameIfVirtual);
		//log.info("HOW EXCITING!!!  WE GOT A RELATIONAL ROW! for table "+tableNameIfVirtual+" keyColumn = "+idColName+" value="+idValue);
	
		Map<String, Object> values = new HashMap<String, Object>();
		for (IColumn col:columns.values()) {    		
			byte[] namearray = new byte[col.name().remaining()];
    		col.name().get(namearray);
    		byte[] valuearray = new byte[col.value().remaining()];
    		col.value().get(valuearray);
			String colName = playorm.bytesToString(namearray); 
			try {
				Object objVal = playorm.sourceConvertFromBytes(tableNameIfVirtual, colName, valuearray);
				values.put(colName, objVal);
			} catch(RuntimeException e) {
				log.warn("Exception converting table="+tableNameIfVirtual+" col="+colName);
				word.set("FAILURE-"+tableNameIfVirtual);
				context.write(word, one);
			}
		}
		String pkValue = playorm.getSourceIdColumnValue(tableNameIfVirtual, key);

		boolean written = playorm.postNormalTable(values, tableNameIfVirtual, pkValue);
		
		if(written) {
			word.set("totalrelationalwritten");
			context.write(word, one);
		} else {
			word.set("totalrelationalskipped");
			context.write(word, one);
		}
		word.set(tableNameIfVirtual);
		context.write(word, one);
	}


	private void transferStream(byte[] key, SortedMap<ByteBuffer, IColumn> columns, String tableNameIfVirtual, Context context) throws IOException, InterruptedException {
		String time = playorm.getSourceIdColumnValue(tableNameIfVirtual, key);
		String valueAsString = null;

		if(columns.size() != 1)
			throw new RuntimeException("BIG ISSUE, column size="+columns.size()+" but should only have a value column");

        word.set("totalread222");
        context.write(word, one);

		//we are only in here because this is a stream, there is only one column and it's name is "value":
		for (IColumn col:columns.values()) {
			byte[] nameArray = new byte[col.name().remaining()];
    		byte[] valuearray = new byte[col.value().remaining()];
    		col.value().get(valuearray);
    		col.name().get(nameArray);

    		String colName = StandardConverters.convertFromBytes(String.class, nameArray);
    		if(!"value".equals(colName))
    			throw new RuntimeException("issue in that column name is not 'value'  name="+colName);

    		try {
    			valueAsString = ""+playorm.sourceConvertFromBytes(tableNameIfVirtual, "value", valuearray);
    			//try to account for every case of 'null' or empty we can think of:
    			if (valueAsString == null || "".equals(valueAsString) || "null".equalsIgnoreCase(valueAsString)) {
    				String hex = StandardConverters.convertToString(valueAsString);
    				//log.warn("got a null or empty value in a timeseries! valueAsString is '"+valueAsString+"', tableNameIfVirtual is "+tableNameIfVirtual+" valuearray is "+valuearray+" len="+valuearray.length+" hex="+hex+" time="+time);
    		        word.set("total-null-values");
    		        context.write(word, one);
    				return;
    			}
    		}
    		catch (Exception e) {
    			log.error("failed getting value from bytes!!!!! val[] len is "+valuearray.length+" column is "+colName+" table name is "+tableNameIfVirtual+" now attempting both bigint and bigdec");
    			System.err.println("failed getting value from bytes!!!!! val[] len is "+valuearray.length+" column is "+colName+" table name is "+tableNameIfVirtual+" now attempting both bigint and bigdec");	
    			throw new RuntimeException(e);
    		}
		}
		
		if ((""+Integer.MAX_VALUE).equals(valueAsString)) {
			//log.warn("NOT POSTING TO TIMESERIES BECAUSE VALUE IS Integer.MAX_VALUE!!!! from table='"+ playorm.getSrcTableDesc(tableNameIfVirtual)+" to dest table ' key="+time+", value="+valueAsString+" mapcounter is "+mapcounter);
			word.set(tableNameIfVirtual+" not written because MAX_VALUE");
	        context.write(word, one);
	        return;
		}
        
		boolean written = playorm.postTimeSeriesToDest(tableNameIfVirtual, time, valueAsString);
        if(written) {
    		//word.set(tableNameIfVirtual);
            //context.write(word, one);

        	word.set("totalwritten");
        	context.write(word, one);
        } else {
        	word.set(tableNameIfVirtual+" not written, no table found");
        	context.write(word, one);
        }
	}
	
	public void cleanup() {
		playorm.flushAll();
	}

}
