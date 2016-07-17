package com.ecommerce.useranalyze.confmanager;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * 
 * 可以在第一次访问它的时候，就从对应的properties文件中，读取配置项，并提供外界获取某个配置key对应的value的方法
 * 
 * @author hz
 *
 */
public class ConfigurationManager {
	
	//将 Properties对象使用private来修饰，避免外部代码错误修改Properties中某个key对应的value，而引发程序错误。

	private static Properties prop = new Properties();
	
	/**
	 * 在静态代码块中，编写读取配置文件的代码，配置文件只会加载一次，可以重复使用
	 */
	static {
		try {
			// 1.通过一个“类名.class”的方式，获取到这个类在JVM中对应的Class对象
			// 2.通过这个Class对象的getClassLoader()方法，获取到当初加载这个类的JVM
			// 中的类加载器（ClassLoader）
			// 3.调用ClassLoader的getResourceAsStream()这个方法
			//   使用类加载器，去加载类加载路径中的指定的文件
			// 4.获取到一个，针对指定文件的输入流（InputStream）
			InputStream in = ConfigurationManager.class
					.getClassLoader().getResourceAsStream("conf.properties"); 
			
			// 调用Properties的load()方法将文件中的符合“key=value”格式的配置项
			//都加载到Properties对象中
			prop.load(in);  
		} catch (Exception e) {
			e.printStackTrace();  
		}
	}
	
	/**
	 * 获取指定key对应的value
	 * @param key 
	 * @return value
	 */
	public static String getProperty(String key) {
		return prop.getProperty(key);
	}
	
	/**
	 * 获取整数类型的配置项
	 * @param key
	 * @return value
	 */
	public static Integer getInteger(String key) {
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 获取布尔类型的配置项
	 * @param key
	 * @return value
	 */
	public static Boolean getBoolean(String key) {
		String value = getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 获取Long类型的配置项
	 * @param key
	 * @return
	 */
	public static Long getLong(String key) {
		String value = getProperty(key);
		try {
			return Long.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0L;
	}
	
}
