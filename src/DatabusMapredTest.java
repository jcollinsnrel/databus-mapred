
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

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
import org.apache.hadoop.mapreduce.Reducer;
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

	
		try {
	       
		
	        Job job = new Job(getConf(), "databusmapredtest");
	        job.setJarByClass(DatabusMapredTest.class);
	        job.setMapperClass(DatabusCopyToNewSchemaMapper.class);
	
	        //these setting as specific to this debugger reducer that just ouputs a count of all tables written to:
	        job.setCombinerClass(ReducerToFilesystem.class);
            job.setReducerClass(ReducerToFilesystem.class);
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
	        //job.setNumReduceTasks(1);
	
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
	        ConfigHelper.setThriftFramedTransportSizeInMb(job.getConfiguration(), 100);
	
	        job.waitForCompletion(true);
	        return 0;
		}
		finally {
    		Thread.currentThread().setContextClassLoader(oldCl);

		}
    }
    
    public static Charset charset = Charset.forName("UTF-8");

    public static ByteBuffer str_to_bb(String msg){
      try{
        return charset.newEncoder().encode(CharBuffer.wrap(msg));
      }catch(Exception e){e.printStackTrace();}
      return null;
    }
    
    public static class ReducerToFilesystem extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            context.write(key, new IntWritable(sum));
        }
    }

}
