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
		
		
		
		// reverse the logic.  try myself first, then delgate:
		 Class c = findLoadedClass(name);
		 if (c == null) {
			 c = findClass(name);
			 if (c == null) {
				 System.out.println("No!  classloader"+this+" didn't have "+name+" delegating to parent!");
				 c = super.loadClass(name, resolve);
			 }
			 else {
				 System.out.println("Yes!  classloader"+this+" did have "+name);
			 }
		 }
		 if (resolve) {
		     resolveClass(c);
		 }
		 System.out.println("found class "+name+".  I am "+this+" it's classloader is "+c.getClassLoader()+" it's codesource it "+c.getProtectionDomain().getCodeSource());

		 return c;
	}
	
	@Override
	public String toString() {
		return "TestClassloader named "+clname+" super: "+super.toString();
	}

}
