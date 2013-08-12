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
import java.util.Map.Entry;
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
        	}
        	String columnString = "";
        	for (Entry<ByteBuffer, IColumn> entry:columns.entrySet()) {
        		byte[] thiskey = new byte[entry.getKey().remaining()];
        		entry.getKey().get(thiskey);
        		byte[] thisvalue = new byte[entry.getValue().name().remaining()];
        		entry.getValue().name().get(thisvalue);
        		columnString = columnString+"key="+thiskey+",columnName="+thisvalue+":";
        	}
    		log.info("posting to timeseries table='"+ tableNameIfVirtual +"' key="+key+", mapcounter is "+mapcounter+" columnString is "+columnString);
			word.set(tableNameIfVirtual);
            context.write(word, one);
        }
        
        public static String fetchTableNameIfVirtual(byte[] virtKey) {
    		int tableLen = virtKey[0] & 0xFF;
    		byte[] bytesName = Arrays.copyOfRange(virtKey, 1, tableLen+1);
    		String s = new String(bytesName);
    		return s;
    	}
        

    }