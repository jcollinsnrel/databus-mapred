
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DatabusMapredTest extends Configured implements Tool
{
    static final String KEYSPACE = "databus5";
    static final String COLUMN_FAMILY = "nreldata";

    static final String OUTPUT_REDUCER_VAR = "output_reducer";
    static final String OUTPUT_COLUMN_FAMILY = "output_words";
    private static final String OUTPUT_PATH_PREFIX = "/tmp/data_count";

//    private static final String CONF_COLUMN_NAME = "columnname";

    public static void main(String[] args) throws Exception
    {
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new DatabusMapredTest(), args);
        System.exit(0);
    }

    public static class TokenizerMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private ByteBuffer sourceColumn;

        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException
        {
        }

        @Override
        public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
        {
        	super.map(key, columns, context);
            for (IColumn column : columns.values())
            {
                String name  = ByteBufferUtil.string(column.name());
                //value = ByteBufferUtil.string(column.value());
                         
                word.set(name);
                context.write(word, one);                
            }
        }
    }

    public static class ReducerToLogger extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            context.write(key, new IntWritable(sum));
        }
    }

    
    public int run(String[] args) throws Exception
    {
        String outputReducerType = "logger";
        if (args != null && args[0].startsWith(OUTPUT_REDUCER_VAR))
        {
            String[] s = args[0].split("=");
            if (s != null && s.length == 2)
                outputReducerType = s[1];
        }
        
        // use a smaller page size that doesn't divide the row count evenly to exercise the paging logic better
        ConfigHelper.setRangeBatchSize(getConf(), 99);

//        for (int i = 0; i < WordCountSetup.TEST_COUNT; i++)
//        {
//            String columnName = "text" + i;
            String columnName = "text";

            Job job = new Job(getConf(), "databusmapredtest");
            job.setJarByClass(DatabusMapredTest.class);
            job.setMapperClass(TokenizerMapper.class);

            if (outputReducerType.equalsIgnoreCase("logger"))
            {
                job.setCombinerClass(ReducerToLogger.class);
                job.setReducerClass(ReducerToLogger.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
//                FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX + i));
                FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX));

            }
            else
            {
            	throw new RuntimeException("only logger is supported");
//                job.setReducerClass(ReducerToCassandra.class);
//
//                job.setMapOutputKeyClass(Text.class);
//                job.setMapOutputValueClass(IntWritable.class);
//                job.setOutputKeyClass(ByteBuffer.class);
//                job.setOutputValueClass(List.class);
//
//                job.setOutputFormatClass(ColumnFamilyOutputFormat.class);
//
//                ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KEYSPACE, OUTPUT_COLUMN_FAMILY);
//                job.getConfiguration().set(CONF_COLUMN_NAME, "sum");
            }

            job.setInputFormatClass(ColumnFamilyInputFormat.class);

            ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
            ConfigHelper.setInputInitialAddress(job.getConfiguration(), "sdi-prod-01");
            ConfigHelper.setInputPartitioner(job.getConfiguration(), "RandomPartitioner");
             // this will cause the predicate to be ignored in favor of scanning everything as a wide row
            ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY, true);
            SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes(columnName)));
            ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

            ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "sdi-prod-01");
            ConfigHelper.setOutputPartitioner(job.getConfiguration(), "RandomPartitioner");

            job.waitForCompletion(true);
//        }
        return 0;
    }
    
    
    
    
    
    
    
    
    
    
//    public static class ReducerToCassandra extends Reducer<Text, IntWritable, ByteBuffer, List<Mutation>>
//    {
//        private ByteBuffer outputKey;
//
//        protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
//        throws IOException, InterruptedException
//        {
//            outputKey = ByteBufferUtil.bytes(context.getConfiguration().get(CONF_COLUMN_NAME));
//        }
//
//        public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
//        {
//            int sum = 0;
//            for (IntWritable val : values)
//                sum += val.get();
//            context.write(outputKey, Collections.singletonList(getMutation(word, sum)));
//        }
//
//        private static Mutation getMutation(Text word, int sum)
//        {
//            Column c = new Column();
//            c.setName(Arrays.copyOf(word.getBytes(), word.getLength()));
//            c.setValue(ByteBufferUtil.bytes(sum));
//            c.setTimestamp(System.currentTimeMillis());
//
//            Mutation m = new Mutation();
//            m.setColumn_or_supercolumn(new ColumnOrSuperColumn());
//            m.column_or_supercolumn.setColumn(c);
//            return m;
//        }
//    }

}
