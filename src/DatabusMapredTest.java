
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.base.Bootstrap;
import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.NoSqlEntityManagerFactory;
import com.alvazan.orm.api.base.anno.NoSqlConverter;
import com.alvazan.orm.api.z3api.NoSqlTypedSession;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.conv.Converter;
import com.alvazan.orm.api.z8spi.meta.DboColumnIdMeta;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.ReflectionUtil;
import com.alvazan.orm.api.z8spi.meta.TypedRow;
import com.alvazan.orm.layer9z.spi.db.inmemory.RowImpl;


public class DatabusMapredTest extends Configured implements Tool
{
	static final Logger log = LoggerFactory.getLogger(DatabusMapredTest.class);
    static final String KEYSPACE = "databus5";
    static final String COLUMN_FAMILY = "nreldata";

    static final String OUTPUT_REDUCER_VAR = "output_reducer";
    static final String OUTPUT_COLUMN_FAMILY = "output_words";
    private static final String OUTPUT_PATH_PREFIX = "/tmp/data_count2";

//    private static final String CONF_COLUMN_NAME = "columnname";

    public static void main(String[] args) throws Exception
    {
    	log.info("printing params111!!!!!!!");
    	for (String s:args)
    		System.out.println(s +"111!!!!!!!");
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new DatabusMapredTest(), args);
        System.exit(0);
    }

    public static class TokenizerMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable>
    {
    	static final Logger log = LoggerFactory.getLogger(DatabusMapredTest.class);
    	static long mapcounter=0;
    	
        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);

        private ByteBuffer sourceColumn;

        static private NoSqlEntityManager sourceMgr;
        static private NoSqlEntityManager destMgr;     
        static private boolean initialized = false;
        static private boolean initializing = false;


        
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException
        {
        	if (destMgr != null)
        		return;
        	while (initializing)
        		Thread.sleep(1);
        	//manual thread locking!  Why not.
        	if (!initialized) {
        		if (initializing) {
        			while(initializing) Thread.sleep(2);
        			return;
        		}
        		initializing=true;
	            //outputKey = ByteBufferUtil.bytes(context.getConfiguration().get(CONF_COLUMN_NAME));
	        	log.info("in reducerToCassandra setup11!!!!!!!");
	    		String cluster1 = "TestCluster";
	    		String seeds1 = "sdi-prod-01:9160,sdi-prod-02:9160,sdi-prod-03:9160,sdi-prod-04:9160";
	    		String port1 = "9160";
	    		
	    		String cluster2 = "TestCluster";
	    		String seeds2 = "sdi-prod-01:9160,sdi-prod-02:9160,sdi-prod-03:9160,sdi-prod-04:9160";
	    		String port2 = "9160";
	
	//            List<Class> classes = Play.classloader.getAnnotatedClasses(NoSqlEntity.class);
	//            List<Class> classEmbeddables = Play.classloader.getAnnotatedClasses(NoSqlEmbeddable.class);
	//            classes.addAll(classEmbeddables);
	            
	    		Map<String, Object> props = new HashMap<String, Object>();
	    		props.put(Bootstrap.TYPE, "cassandra");
	    		props.put(Bootstrap.CASSANDRA_KEYSPACE, KEYSPACE);
	    		props.put(Bootstrap.CASSANDRA_CLUSTERNAME, cluster1);
	    		props.put(Bootstrap.CASSANDRA_SEEDS, seeds1);
	    		props.put(Bootstrap.CASSANDRA_THRIFT_PORT, port1);
	    		props.put(Bootstrap.AUTO_CREATE_KEY, "create");
	    		//props.put(Bootstrap.LIST_OF_EXTRA_CLASSES_TO_SCAN_KEY, classes);
	
	    		NoSqlEntityManagerFactory factory1 = Bootstrap.create(props, Thread.currentThread().getContextClassLoader());  //that 'null' is a classloader that supposed to come from play...  does null work?
	    		sourceMgr = factory1.createEntityManager();
	    		
	    		Map<String, Object> props2 = new HashMap<String, Object>();
	    		props2.put(Bootstrap.TYPE, "cassandra");
	    		props2.put(Bootstrap.CASSANDRA_KEYSPACE, KEYSPACE);
	    		props2.put(Bootstrap.CASSANDRA_CLUSTERNAME, cluster2);
	    		props2.put(Bootstrap.CASSANDRA_SEEDS, seeds2);
	    		props2.put(Bootstrap.CASSANDRA_THRIFT_PORT, port2);
	    		props2.put(Bootstrap.AUTO_CREATE_KEY, "create");
	    		//props2.put(Bootstrap.LIST_OF_EXTRA_CLASSES_TO_SCAN_KEY, classes);
	
	    		NoSqlEntityManagerFactory factory2 = Bootstrap.create(props2, Thread.currentThread().getContextClassLoader());  //that 'null' is a classloader that supposed to come from play...  does null work?
	    		destMgr = factory2.createEntityManager();
	    		initialized = true;
	    		initializing=false;
        	}
            
            
            
        }

        @Override
        public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
        {
        	mapcounter++;
        	//only do every 10th one:
        	if (mapcounter%10!=1)
        		return;
        	if (mapcounter%1000 == 1) {
        		log.info("called map "+mapcounter+" times.");
        		context.progress();
        	}
        	//super.map(key, columns, context);

        	
        	NoSqlTypedSession session = sourceMgr.getTypedSession();
    		NoSqlTypedSession session2 = destMgr.getTypedSession();
        	//NoSqlSession raw = session.getRawSession();
    		//NoSqlSession raw2 = session2.getRawSession();
    		
    		String tableNameIfVirtual = DboColumnIdMeta.fetchTableNameIfVirtual(key.array());
    		
    		log.info("tableNameIfVirtual is "+tableNameIfVirtual);
    		System.out.println("tableNameIfVirtual is "+tableNameIfVirtual);
    		System.err.println("tableNameIfVirtual is "+tableNameIfVirtual);
    		
    		DboTableMeta meta = sourceMgr.find(DboTableMeta.class, tableNameIfVirtual);
    		//DboTableMeta meta2 = destMgr.find(DboTableMeta.class, tableNameIfVirtual+"StreamTrans");
    		
    		//eventually you want to do this:
    		List<com.alvazan.orm.api.z8spi.action.Column> cols = new ArrayList<com.alvazan.orm.api.z8spi.action.Column>();
			TreeMap<ByteArray, com.alvazan.orm.api.z8spi.action.Column> colTree = new TreeMap<ByteArray, com.alvazan.orm.api.z8spi.action.Column>();
    		
    		for (IColumn col:columns.values()) {    		
    			com.alvazan.orm.api.z8spi.action.Column pormCol = new com.alvazan.orm.api.z8spi.action.Column(col.name().array(), col.value().array());
    			pormCol.getName();
    			
    			cols.add(pormCol);
    			
    		}
            RowImpl row = new RowImpl(colTree);
            row.setKey(key.array());
            KeyValue<TypedRow> keyVal = meta.translateFromRow(row);

    		System.out.println("posting to timeseries table='"+ tableNameIfVirtual +"' key="+keyVal.getKey()+", value="+keyVal.getValue());
            
    		//postTimeSeries(meta2, keyVal.getKey(), keyVal.getValue(), session2);
        }
        
        private static void postTimeSeries(DboTableMeta table, Object pkValue, Object value, NoSqlTypedSession typedSession) {

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
    }

    public static class ReducerToLogger extends Reducer<Text, IntWritable, Text, IntWritable>
    {
    	static final Logger log = LoggerFactory.getLogger(DatabusMapredTest.class);

    	static long reducecounter=0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
        	reducecounter++;
        	if (reducecounter%1000 == 1)
        		log.info("called reduce "+reducecounter+" times.");
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            context.write(key, new IntWritable(sum));
        }
    }

    
    public int run(String[] args) throws Exception
    {        
        // use a smaller page size that doesn't divide the row count evenly to exercise the paging logic better
        ConfigHelper.setRangeBatchSize(getConf(), 99);

        String columnName = "value";

        Job job = new Job(getConf(), "databusmapredtest");
        job.setJarByClass(DatabusMapredTest.class);
        job.setMapperClass(TokenizerMapper.class);

        job.setCombinerClass(ReducerToLogger.class);
        job.setReducerClass(ReducerToLogger.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX));
        Configuration config = new Configuration();
    	FileSystem hdfs = FileSystem.get(config);
    	Path srcPath = new Path(OUTPUT_PATH_PREFIX);
    	if (hdfs.exists(srcPath))
    		hdfs.delete(srcPath, true);
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX));
        
        

        job.setInputFormatClass(ColumnFamilyInputFormat.class);
        log.info("number of reduce tasks:  "+job.getNumReduceTasks());
        job.setNumReduceTasks(3);
        log.info("updated, now number of reduce tasks:  "+job.getNumReduceTasks());

        ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
        ConfigHelper.setInputInitialAddress(job.getConfiguration(), "sdi-prod-01");
        ConfigHelper.setInputPartitioner(job.getConfiguration(), "RandomPartitioner");
         // this will cause the predicate to be ignored in favor of scanning everything as a wide row
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY, true);
        SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes(columnName)));
        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

        int rangebatchsize = 1024;
        log.info("setting rangeBatchSize to "+rangebatchsize);
        ConfigHelper.setRangeBatchSize(job.getConfiguration(), rangebatchsize);

        job.waitForCompletion(true);
        return 0;
    }

}
