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

}
