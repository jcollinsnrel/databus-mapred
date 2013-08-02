
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.SortedMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



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

        static private IPlayormContext playorm = null;
        static private boolean initialized = false;
        static private boolean initializing = false;
        
        static URLClassLoader interfacecl = null;
    	static URLClassLoader hadoopcl = null;
    	static URLClassLoader playormcontextcl = null;


        
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException
        {
        	if (playorm != null)
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
	            
	    		try{
	    			Class mainClass = playormcontextcl.loadClass("PlayormContext");
	    			playorm = (IPlayormContext) mainClass.newInstance();
	    			playorm.initialize(KEYSPACE, cluster1, seeds1, port1, KEYSPACE, cluster2, seeds2, port2);
	    		}
	    		catch (Exception e) {
	    			e.printStackTrace();
	    			log.error("got exception loading playorm!  "+e.getMessage());
	    		}
	    		finally {
	    			//Thread.currentThread().setContextClassLoader(interfacecl);
	    		}
	    		initialized = true;
	    		initializing=false;
        	}
        	setupHadoopClassloader();
                
        }
        
        private void setupHadoopClassloader() {
    		ClassLoader oldCl = Thread.currentThread().getContextClassLoader();
    		
    		try{
    			CodeSource src = DatabusMapredTest.class.getProtectionDomain().getCodeSource();
    	
    			//interfacecl will be the parent of both the hadoopcl and the playormcontextcl, 
    			//it will have only the IPlayormContext class added to the bootstrap classloader
    			List<URL> playormcontextclurls = new ArrayList<URL>();
    			List<URL> interfaceclurls = new ArrayList<URL>();  
    			List<URL> hadoopclurls = new ArrayList<URL>();  
    	
    			URL location = src.getLocation();
    			interfaceclurls.add(location);
    	        log.info("******** location from codesource is "+location);
    	        File libdir = new File(location.getPath()+"lib/");
    	        File cassandra126libsdir = new File(location.getPath()+"classes/libcassandra1.2.6");
    	        log.info("******** libdir absolute is "+libdir.getAbsolutePath());
    	        log.info("******** libdir tostring is "+libdir);
    	        log.info("******** libdir name is "+libdir.getName());
    	        log.info("******** libdir cannonical is "+libdir.getCanonicalPath());
    	        
    	        log.info("******** cassandra126libdir absolute is "+cassandra126libsdir.getAbsolutePath());
    	        log.info("******** cassandra126libdir tostring is "+cassandra126libsdir);
    	        log.info("******** cassandra126libdir name is "+cassandra126libsdir.getName());
    	        log.info("******** cassandra126libdir cannonical is "+cassandra126libsdir.getCanonicalPath());
    	
    	        
    	        
    	        log.info("******** libdir.listfiles() is "+Arrays.toString(libdir.listFiles()));
    	        for (File f : libdir.listFiles()) {
    	        	if (f.getName().contains(".jar") && !f.getName().equals("cassandra-all-1.2.6.jar") && !f.getName().equals("cassandra-thrift-1.2.6.jar"))
    	            	interfaceclurls.add(f.toURL());
    	        }
    	        
    	        for (File f : cassandra126libsdir.listFiles()) {
    	       		hadoopclurls.add(f.toURL());
    	        }
    	        
    	        
    	        log.info("******** interfaceclurls is: "+Arrays.toString(interfaceclurls.toArray(new URL[]{})));
    	        log.info("******** hadoopclurls is: "+Arrays.toString(hadoopclurls.toArray(new URL[]{})));
    	        log.info("******** playormcontextclurls is: "+Arrays.toString(playormcontextclurls.toArray(new URL[]{})));
    	
    	        
    			interfacecl =
    	                new URLClassLoader(
    	                		interfaceclurls.toArray(new URL[0]),
    	                        ClassLoader.getSystemClassLoader());
    			playormcontextcl =
    	                new URLClassLoader(
    	                        playormcontextclurls.toArray(new URL[0]),
    	                        interfacecl);
    			hadoopcl =
    	                new URLClassLoader(
    	                        hadoopclurls.toArray(new URL[0]),
    	                        interfacecl);
    			log.info(" ======  the interfacecl (shared parent) urls are "+Arrays.toString(interfacecl.getURLs()));
    			log.info("about to print resources for org.apache.thrift.transport.TTransport");
    			for (Enumeration<URL> resources = interfacecl.findResources("org.apache.thrift.transport.TTransport"); resources.hasMoreElements();) {
    			       log.info("a resource is "+resources.nextElement());
    			}
    			log.info("done printing resources");
    		
        		log.info("system classloader is "+ClassLoader.getSystemClassLoader());
        		log.info("interfacecl classloader is "+interfacecl);
        		log.info("the playormcontext classloader is "+playormcontextcl);
        		log.info("the hadoop classloader is "+hadoopcl);

        		log.info("the current classloader is "+oldCl);
        		log.info("interfacecl classloader parent is "+interfacecl.getParent());
        		log.info("the playormcontext classloader parent is (should be same as line above)"+playormcontextcl.getParent());
        		log.info("the hadoop classloader parent is (should be the same as 2 lines above)"+hadoopcl.getParent());

        		log.info("the current (old) classloader parent is "+oldCl.getParent());
        		//ClassLoader.getSystemClassLoader().getParent()
        		Class interfaceclass = interfacecl.loadClass("IPlayormContext");

        		log.info("the owner of interfaceclass is (should be same as 3 lines above)"+interfaceclass.getClassLoader());

    			log.info("about to try to load org.apache.thrift.transport.TTransport");
        		Class c = playormcontextcl.loadClass("org.apache.thrift.transport.TTransport");
        		log.info("loaded org.apache.thrift.transport.TTransport, class is "+c);
    			
    			
    			Thread.currentThread().setContextClassLoader(playormcontextcl);
    			
    		}
    		catch (Exception e) {
    			e.printStackTrace();
    			log.error("got exception loading playorm!  "+e.getMessage());
    		}
    		finally {
    			Thread.currentThread().setContextClassLoader(oldCl);
    		}
        }

        @Override
        public void map(ByteBuffer keyData, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
        {
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
				

//    			try {
//    				n = StandardConverters.convertFromBytes(BigDecimal.class, valuearray);
//    			}
//    			catch (Exception e) {
//    				System.err.println(" -- got an exception trying to convert value to BD, it's not a BD!");
//    			}
//    			try {
//    				n = StandardConverters.convertFromBytes(BigInteger.class, valuearray);
//    			}
//    			catch (Exception e) {
//    				System.err.println(" -- got an exception trying to convert value to BI, it's not a BI!");
//    			}
    			
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
		ClassLoader oldCl = Thread.currentThread().getContextClassLoader();

		try {
			ClassLoader hadoopcl = setupHadoopClassloader();
			Thread.currentThread().setContextClassLoader(hadoopcl);
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
	        //SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes(columnName)));
	        SlicePredicate predicate = new SlicePredicate();
	        SliceRange sliceRange = new SliceRange();
	        sliceRange.setStart(new byte[0]);
	        sliceRange.setFinish(new byte[0]);
	        predicate.setSlice_range(sliceRange);
	        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);
	
	        int rangebatchsize = 1024;
	        log.info("setting rangeBatchSize to "+rangebatchsize);
	        ConfigHelper.setRangeBatchSize(job.getConfiguration(), rangebatchsize);
	        //ConfigHelper.setThriftMaxMessageLengthInMb(job.getConfiguration(), 100);
	        ConfigHelper.setThriftFramedTransportSizeInMb(job.getConfiguration(), 100);
	
	        job.waitForCompletion(true);
	        return 0;
		}
		finally {
    		Thread.currentThread().setContextClassLoader(oldCl);

		}
    }
    
    
    private ClassLoader setupHadoopClassloader() {		
		try{
			CodeSource src = DatabusMapredTest.class.getProtectionDomain().getCodeSource();

			List<URL> hadoopclurls = new ArrayList<URL>();
			URL location = src.getLocation(); 
	        File cassandra126libsdir = new File("jar:"+location.getPath()+"!libcassandra1.2.6");
	        System.err.println("cassandra126libsdir is "+cassandra126libsdir.getPath());
	       
	        for (File f : cassandra126libsdir.listFiles()) {
	       		hadoopclurls.add(f.toURL());
	        }
	
	        
			URLClassLoader hadoopcl =
	                new URLClassLoader(
	                        hadoopclurls.toArray(new URL[0]),
	                        ClassLoader.getSystemClassLoader());
			return hadoopcl;
			
		}
		catch (Exception e) {
			e.printStackTrace();
			log.error("got exception loading playorm!  "+e.getMessage());
			return null;
		}
    }



}
