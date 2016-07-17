/**
 * 
 *	TODO  
 *1、配置管理组件，可以在读取大量的配置信息之后，用单例模式的方式，就将配置信息仅仅保存在一个实例的
 * 实例变量中，这样可以避免对于静态不变的配置信息，反复多次的读取
 * 2、JDBC辅助组件，全局就只有一个实例，实例中持有了一个内部的简单数据源
 * 使用了单例模式之后，就保证只有一个实例，那么数据源也只有一个，不会重复创建多次数据源（数据库连接池）
 */
package com.ecommerce.useranalyze.test;



/**
 * 单例模式测试类
 * @author hz
 *
 */
public class SingletonMode {
	
	//声明私有的静态变量，来引用即将被创建出来的单例
	private  static SingletonMode instance = null;
	
	//构造方法使用private进行私有化
	private SingletonMode(){
		
	}
	public static SingletonMode getInstance() {
		// 两步检查机制
		// 1.多个线程过来的时候，判断instance是否为null,如果为null再往下走
		if(instance == null) {
			// 在这里，进行多个线程的同步
			// 同一时间，只能有一个线程获取到Singleton Class对象的锁
			// 进入后续的代码
			// 其他线程，都是只能够在原地等待，获取锁
			synchronized(SingletonMode.class) {
				// 2.只有第一个获取到锁的线程，进入到这里，会发现是instance是null
				// 然后才会去创建这个单例
				// 此后的线程，因为instance不是null，就不会反复创建一个单例
				if(instance == null) {
					instance = new SingletonMode();
				}
			}
		}
		return instance;
	}

}
