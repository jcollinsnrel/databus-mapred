import java.net.URL;
import java.net.URLClassLoader;


public class TestClassloader extends URLClassLoader {

	public TestClassloader(URL[] urls, ClassLoader parent) {
		super(urls, parent);
	}
	
	protected Class<?> findClass(final String name)
        throws ClassNotFoundException {
		System.out.println("calling findClass with class "+name+".  I am "+this);
		return super.findClass(name);
	}
	
	public Class<?> loadClass(String name)
	         throws ClassNotFoundException
	{
		System.out.println("calling loadClass with class "+name+".  I am "+this);
		return super.loadClass(name);
	}
	
	public Class<?> loadClass(String name, boolean resolve)
	         throws ClassNotFoundException
	{
		System.out.println("calling loadClass boolean with class "+name+".  I am "+this);
		return super.loadClass(name, resolve);
	}

}
