import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabusCopyToNewSchemaMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable>
    {
    	static final Logger log = LoggerFactory.getLogger(DatabusMapredTest.class);
        
    	public Text word;
    	public ByteBuffer sourceColumn;

        
        static private boolean initialized = false;
        static private boolean initializing = false;
        private static Object delegate = null;
        private static Class delegateClass = null;
        
        static TestClassloader interfacecl = null;
    	static TestClassloader hadoopcl = null;
    	static TestClassloader playormcontextcl = null;

        
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
        throws IOException, InterruptedException
        {
        	if (delegate != null)
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

	        	try{
	            	setupHadoopClassloader();
	            	System.out.println("setting the current thread classloader to "+playormcontextcl+" this thread is "+Thread.currentThread());
	            	Thread.currentThread().setContextClassLoader(playormcontextcl);
	            	context.getConfiguration().setClassLoader(playormcontextcl);
	            	delegateClass = playormcontextcl.loadClass("DatabusCopyMapperImpl");
	            	delegate = delegateClass.newInstance();
	            	
	        	}
	        	catch (Exception e) {
	        		e.printStackTrace();
	        		log.error("failed setting up HadoopClassloader()");
	        	}
	    		initialized = true;
	    		initializing=false;
        	}
                
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
    	        interfaceclurls.add(libdir.toURL());

    	        File interfacelibdir = new File(location.getPath()+"libvar/commonInterface");
    	        
    	        File playormlibdir = new File(location.getPath()+"libvar/playormLib");

    	        log.info("******** libdir absolute is "+libdir.getAbsolutePath());
    	        log.info("******** libdir tostring is "+libdir);
    	        log.info("******** libdir name is "+libdir.getName());
    	        log.info("******** libdir cannonical is "+libdir.getCanonicalPath());
    	        log.info("******** libdir.listfiles() is "+Arrays.toString(libdir.listFiles()));
    	        
    	        log.info("******** interfacelibdir cannonical is "+interfacelibdir.getCanonicalPath());
    	        log.info("******** interfacelibdir exists is "+interfacelibdir.exists());
    	        log.info("******** interfacelibdir.listfiles() is "+Arrays.toString(interfacelibdir.listFiles()));
    	        
    	        log.info("******** playormlibdir cannonical is "+playormlibdir.getCanonicalPath());
    	        log.info("******** playormlibdir exists is "+playormlibdir.exists());
    	        log.info("******** playormlibdir.listfiles() is "+Arrays.toString(playormlibdir.listFiles()));
//    	        for (File f : libdir.listFiles()) {
//    	        	if (f.getName().contains(".jar") && !f.getName().equals("cassandra-all-1.2.6.jar") && !f.getName().equals("cassandra-thrift-1.2.6.jar"))
//    	            	interfaceclurls.add(f.toURL());
//    	        }
//    	        
//    	        interfaceclurls.add(interfacelibdir.toURL());
//    	        for (File f : interfacelibdir.listFiles()) {
//    	            interfaceclurls.add(f.toURL());
//    	        	
//    	        }
    	        
    	        for (File f : libdir.listFiles()) {
    	        	if (f.getName().equals("cassandra-all-1.2.6.jar") || f.getName().equals("cassandra-thrift-1.2.6.jar"))
    	            	hadoopclurls.add(f.toURL());
    	        }
    	        
    	        playormcontextclurls.add(playormlibdir.toURL());
    	        for (File f : playormlibdir.listFiles()) {
    	        	playormcontextclurls.add(f.toURL());
    	        }
    	        
    	        
    	        
    	        log.info("******** interfaceclurls is: "+Arrays.toString(interfaceclurls.toArray(new URL[]{})));
    	        log.info("******** hadoopclurls is: "+Arrays.toString(hadoopclurls.toArray(new URL[]{})));
    	        log.info("******** playormcontextclurls is: "+Arrays.toString(playormcontextclurls.toArray(new URL[]{})));
    	
    	        
    			interfacecl =
    	                new TestClassloader(
    	                		interfaceclurls.toArray(new URL[0]),
    	                        ClassLoader.getSystemClassLoader());
    			interfacecl.setName("interfaceclassloader");
    			playormcontextcl =
    	                new TestClassloader(
    	                        playormcontextclurls.toArray(new URL[0]),
    	                        interfacecl);
    			playormcontextcl.setName("playormclassloader");
    			hadoopcl =
    	                new TestClassloader(
    	                        hadoopclurls.toArray(new URL[0]),
    	                        interfacecl);
    			hadoopcl.setName("hadoopclassloader");
    			log.info(" ======  the interfacecl (shared parent) urls are "+Arrays.toString(interfacecl.getURLs()));
    			log.info("about to print resources for org.apache.thrift.transport.TTransport");
    			for (Enumeration<URL> resources = interfacecl.findResources("org.apache.thrift.transport.TTransport"); resources.hasMoreElements();) {
    			       log.info("a resource is "+resources.nextElement());
    			}
    			log.info("done printing resources");
    		
        		log.info("system classloader is "+ClassLoader.getSystemClassLoader());
        		log.info("the playormcontext classloader is "+playormcontextcl);
        		log.info("the hadoop classloader is "+hadoopcl);

        		log.info("the current classloader is "+oldCl);
        		log.info("interfacecl classloader parent is "+interfacecl.getParent());
        		
        		log.info("interfacecl classloader is "+interfacecl);
        		log.info("the playormcontext classloader parent is (should be same as line above)"+playormcontextcl.getParent());
        		log.info("the hadoop classloader parent is (should be the same as 2 lines above)"+hadoopcl.getParent());

        		log.info("the current (old) classloader parent is "+oldCl.getParent());
        		//ClassLoader.getSystemClassLoader().getParent()
        		//Class interfaceclass = interfacecl.loadClass("IPlayormContext");

        		//log.info("the owner of interfaceclass is (should be same as 3 lines above)"+interfaceclass.getClassLoader());

    			//log.info("about to try to load org.apache.thrift.transport.TTransport");
        		//Class c = playormcontextcl.loadClass("org.apache.thrift.transport.TTransport");
        		//log.info("loaded org.apache.thrift.transport.TTransport, class is "+c);
    			    			
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
        	try {
        		Method mapMethod = delegateClass.getDeclaredMethod("map", ByteBuffer.class, SortedMap.class, Context.class);
        		mapMethod.invoke(delegate, keyData, columns, context);
        	}
        	catch (Exception e) {
        		e.printStackTrace();
        		throw new RuntimeException(e);
        	}
        }

    }