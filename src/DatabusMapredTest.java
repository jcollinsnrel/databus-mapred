
import java.io.File;
import java.net.URL;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
    
    public static void main(String[] args) throws Exception
    {
    	log.info("printing params111!!!!!!!");
    	for (String s:args)
    		System.out.println(s +"111!!!!!!!");
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new DatabusMapredTest(), args);
        System.exit(0);
    }

    

    
    public int run(String[] args) throws Exception
    {        
    	
		ClassLoader oldCl = Thread.currentThread().getContextClassLoader();

		//ClassLoader hadoopcl = setupRunClassloader();
		//System.out.println("-------- settting the classloader to 'hadoopcl' "+hadoopcl);
		//Thread.currentThread().setContextClassLoader(hadoopcl);
		try {
	        // use a smaller page size that doesn't divide the row count evenly to exercise the paging logic better
			//getConf().setClassLoader(hadoopcl);
	        ConfigHelper.setRangeBatchSize(getConf(), 99);
		
	        Job job = new Job(getConf(), "databusmapredtest");
	        job.setJarByClass(DatabusMapredTest.class);
	        job.setMapperClass(DatabusCopyToNewSchemaMapper.class);
	
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
	        job.setNumReduceTasks(3);
	
	        ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
	        ConfigHelper.setInputInitialAddress(job.getConfiguration(), "sdi-prod-01");
	        ConfigHelper.setInputPartitioner(job.getConfiguration(), "RandomPartitioner");
	         // this will cause the predicate to be ignored in favor of scanning everything as a wide row
	        ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY, true);
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
    
    private ClassLoader setupRunClassloader() {
		
		try{
			CodeSource src = DatabusMapredTest.class.getProtectionDomain().getCodeSource();
	
			//interfacecl will be the parent of both the hadoopcl and the playormcontextcl, 
			//it will have only the IPlayormContext class added to the bootstrap classloader
			List<URL> interfaceclurls = new ArrayList<URL>();  
			List<URL> hadoopclurls = new ArrayList<URL>();  
	
			URL location = src.getLocation();
			interfaceclurls.add(location);
	        log.info("-------- location from codesource is "+location);
	        File libdir = new File(location.getPath()+"lib/");

	        log.info("-------- libdir absolute is "+libdir.getAbsolutePath());
	        log.info("-------- libdir tostring is "+libdir);
	        log.info("-------- libdir name is "+libdir.getName());
	        log.info("-------- libdir cannonical is "+libdir.getCanonicalPath());
	
	        
	        
	        log.info("-------- libdir.listfiles() is "+Arrays.toString(libdir.listFiles()));
	        for (File f : libdir.listFiles()) {
	        	if (f.getName().contains(".jar") && !f.getName().equals("cassandra-all-1.2.6.jar") && !f.getName().equals("cassandra-thrift-1.2.6.jar")
	        			&& !f.getName().equals("PlayormContext.class") && !f.getName().equals("playorm.jar"))
	            	interfaceclurls.add(f.toURL());
	        }
	        
	        for (File f : libdir.listFiles()) {
	        	if (f.getName().equals("cassandra-all-1.2.6.jar") || f.getName().equals("cassandra-thrift-1.2.6.jar"))
	            	hadoopclurls.add(f.toURL());
	        }
	        
	        
	        log.info("-------- interfaceclurls is: "+Arrays.toString(interfaceclurls.toArray(new URL[]{})));
	        log.info("-------- hadoopclurls is: "+Arrays.toString(hadoopclurls.toArray(new URL[]{})));
	
	        
	        TestClassloader interfacecl =
	                new TestClassloader(
	                		interfaceclurls.toArray(new URL[0]),
	                        ClassLoader.getSystemClassLoader().getParent());
	        TestClassloader hadoopcl =
	                new TestClassloader(
	                        hadoopclurls.toArray(new URL[0]),
	                        interfacecl);
			log.info("--------  the interfacecl (shared parent) urls are "+Arrays.toString(interfacecl.getURLs()));
			log.info("--------about to print resources for org.apache.thrift.transport.TTransport");
			for (Enumeration<URL> resources = interfacecl.findResources("org.apache.thrift.transport.TTransport"); resources.hasMoreElements();) {
			       log.info("--------a resource is "+resources.nextElement());
			}
			log.info("--------done printing resources");
		
    		log.info("--------system classloader is "+ClassLoader.getSystemClassLoader());
    		log.info("--------the hadoop classloader is "+hadoopcl);

    		log.info("--------interfacecl classloader parent is "+interfacecl.getParent());
    		
    		log.info("--------interfacecl classloader is "+interfacecl);
    		log.info("--------the hadoop classloader parent is (should be the same as 2 lines above)"+hadoopcl.getParent());

    		log.info("--------the current (old) classloader parent is "+ClassLoader.getSystemClassLoader().getParent());
    		//ClassLoader.getSystemClassLoader().getParent()
    		Class interfaceclass = interfacecl.loadClass("IPlayormContext");

    		log.info("--------the owner of interfaceclass is (should be same as 3 lines above)"+interfaceclass.getClassLoader());

			log.info("--------about to try to load org.apache.thrift.transport.TTransport");
    		Class c = hadoopcl.loadClass("org.apache.thrift.transport.TTransport");
    		log.info("--------loaded org.apache.thrift.transport.TTransport, class is "+c);
			return hadoopcl; 			
		}
		catch (Exception e) {
			e.printStackTrace();
			log.error("--------got exception loading playorm!  "+e.getMessage());
		}
		return null;
		
    }

}
