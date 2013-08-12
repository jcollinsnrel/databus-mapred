import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabusCopyToNewSchemaMapper extends Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, Text, IntWritable>
    {
    	static final Logger log = LoggerFactory.getLogger(DatabusCopyToNewSchemaMapper.class);
            	
    	static long mapcounter=0;
    	static final String KEYSPACE = "databus5";
    	
    	private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        //public void map(ByteBuffer keyData, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
        public void map(Map<String, ByteBuffer> keys, Map<String, ByteBuffer> columns, Context context) throws IOException, InterruptedException
        {	
    		byte[] key = new byte[]{};
    		for (Entry<String, ByteBuffer> column : columns.entrySet())
            {
    			log.info("the entry key is "+column.getKey());
    			String value = ByteBufferUtil.string(column.getValue());
    			log.info("read {}:{}={} from {}",
                    new Object[] {toString(keys), column.getKey(), value, context.getInputSplit()});
    			
            }
    		//key = new byte[column.getKey().remaining()];
    		//keyData.get(key);
    		
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
        
        private String toString(Map<String, ByteBuffer> keys)
        {
            String result = "";
            try
            {
                for (ByteBuffer key : keys.values())
                    result = result + ByteBufferUtil.string(key) + ":";
            }
            catch (CharacterCodingException e)
            {
                log.error("Failed to print keys", e);
            }
            return result;
        }
        
        public static String fetchTableNameIfVirtual(byte[] virtKey) {
    		int tableLen = virtKey[0] & 0xFF;
    		byte[] bytesName = Arrays.copyOfRange(virtKey, 1, tableLen+1);
    		String s = new String(bytesName);
    		return s;
    	}
    }