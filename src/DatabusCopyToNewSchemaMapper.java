import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabusCopyToNewSchemaMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable>
    {
    	static final Logger log = LoggerFactory.getLogger(DatabusCopyToNewSchemaMapper.class);
            	
    	static long mapcounter=0;
    	static final String KEYSPACE = "databus5";
    	
    	private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

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
    		log.info("posting to timeseries table='"+ tableNameIfVirtual +"' key="+key+", mapcounter is "+mapcounter);
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