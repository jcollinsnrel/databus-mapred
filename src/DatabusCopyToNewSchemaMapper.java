import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabusCopyToNewSchemaMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable>
    {
    	static final Logger log = LoggerFactory.getLogger(DatabusCopyToNewSchemaMapper.class);
            	
    	static long mapcounter=0;
    	static final String KEYSPACE = "databus5";
    	
    	private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException
        {
        	
                
        }
        
        

        @Override
        public void map(ByteBuffer keyData, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
        {	
    		byte[] key = new byte[keyData.remaining()];
    		keyData.get(key);
    		if (key.length==0) {
    			log.error("GOT A KEY THAT IS SIZE 0!!  WHAT DOES THAT MEAN?");
    			return;
    		}
        	mapcounter++;
        	String tableNameIfVirtual = fetchTableNameIfVirtual(key);
        	//only do every 10th one for testing:
        	if (mapcounter%10!=1) {
        		word.set(tableNameIfVirtual);
                context.write(word, one);
        		return;
        	}
        	if (mapcounter%1000 == 1) {
        		log.info("called map "+mapcounter+" times.");
        		//context.progress();
        	}
    		log.info("posting to timeseries table='"+ tableNameIfVirtual +"' key="+key+", mapcounter is "+mapcounter);
			word.set(tableNameIfVirtual);
            context.write(word, one);
        	//super.map(key, columns, context);

        	//log.info("performing a map, mapcounter is "+mapcounter+" key is "+playorm.bytesToString(key));
    		
//    		if (playorm.sourceTableIsStream(tableNameIfVirtual, key)) {
//    			transferStream(key, columns, tableNameIfVirtual, context);
//    		}
//    		else {
//    			transferOrdinary(key, columns, tableNameIfVirtual, context);
//    		}
    		
        }
        
        public static String fetchTableNameIfVirtual(byte[] virtKey) {
    		int tableLen = virtKey[0] & 0xFF;
    		byte[] bytesName = Arrays.copyOfRange(virtKey, 1, tableLen+1);
    		String s = new String(bytesName);
    		return s;
    	}
        
/*
    	private void transferOrdinary(byte[] key, SortedMap<ByteBuffer, IColumn> columns, String tableNameIfVirtual, Context context) throws IOException, InterruptedException {
    		
    		String idValue = playorm.getSourceIdColumnValue(tableNameIfVirtual, key);
    		String idColName = playorm.getSourceIdColumnName(tableNameIfVirtual);
    		log.info("HOW EXCITING!!!  WE GOT A RELATIONAL ROW! for table "+tableNameIfVirtual+" keyColumn = "+idColName+" value="+idValue);
    	
    		for (IColumn col:columns.values()) {    		
    			byte[] namearray = new byte[col.name().remaining()];
        		col.name().get(namearray);
        		byte[] valuearray = new byte[col.value().remaining()];
        		col.value().get(valuearray);
    			String colName = playorm.bytesToString(namearray); 
    			Object objVal = playorm.sourceConvertFromBytes(tableNameIfVirtual, colName, valuearray);
    			
    			log.info("    "+tableNameIfVirtual+", as strings, A (relational) column is "+ colName+", value "+objVal);
    			word.set(tableNameIfVirtual);
                context.write(word, one);
    		}
    		
    	}


    	private void transferStream(byte[] key, SortedMap<ByteBuffer, IColumn> columns, String tableNameIfVirtual, Context context) throws IOException, InterruptedException {
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
    		log.info("posting to timeseries table='"+ tableNameIfVirtual +"' key="+time+", value="+valueAsString+" mapcounter is "+mapcounter);

    		//TODO!!!!!!  this is just for transfering to the SAME cassandra as a test.  Remove the "Trans" when going to other cassandra instance!
    		//postTimeSeries(tableNameIfVirtual+"Trans", time, value, session2);
    		word.set(tableNameIfVirtual);
            context.write(word, one);
    	}
*/

    }