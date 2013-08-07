import java.net.URL;
import java.net.URLClassLoader;


public class TestClassloader extends URLClassLoader {
	
	private String clname = "";

	public TestClassloader(URL[] urls, ClassLoader parent) {
		super(urls, parent);
	}
	
	public void setName(String name) {
		clname=name;
	}
	
	protected Class<?> findClass(final String name)
        throws ClassNotFoundException {
		System.out.println("calling findClass with class "+name+".  I am "+this);
		Class<?> ret = super.findClass(name);
		System.out.println("found class "+name+" I am "+name+" it's classloader is "+ret.getClassLoader());
		return ret;
	}
	
	public Class<?> loadClass(String name)
	         throws ClassNotFoundException
	{
		System.out.println("calling loadClass with class "+name+".  I am "+this);
		Class<?> ret =  super.loadClass(name);		
		System.out.println("found class "+name+".  I am "+this+" it's classloader is "+ret.getClassLoader()+" it's codesource it "+ret.getProtectionDomain().getCodeSource());

		
		return ret;
	}
	
	public Class<?> loadClass(String name, boolean resolve)
	         throws ClassNotFoundException
	{
		System.out.println("calling loadClass boolean with class "+name+".  I am "+this);
		Class<?> ret = super.loadClass(name, resolve);
		System.out.println("found class "+name+".  I am "+this+" it's classloader is "+ret.getClassLoader()+" it's codesource it "+ret.getProtectionDomain().getCodeSource());

		return ret;
	}
	
	@Override
	public String toString() {
		return "TestClassloader named "+clname+" super: "+super.toString();
	}

}
