/**
 * 
 *	TODO 测试JDBC辅助类
 */
package com.ecommerce.useranalyze.test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ecommerce.useranalyze.jdbchelper.JDBCHelper;

/**
 * @author hz
 *
 */
public class JDBCHelperTest {

	/**hz 
	 * @param args
	 */
	public static void main(String[] args) throws Exception  {
		 // 获取JDBCHelper的单例
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		// 测试普通的增删改语句
		jdbcHelper.executeUpdate(
				"insert into test_user(name,age) values(?,?)", 
				new Object[]{"王二", 28});
	
	// 测试查询语句，将testUser声明为final类型，方便匿名内部类使用
		final Map<String, Object> testUser = new HashMap<String, Object>();
		
		// 1.设计一个内部接口QueryCallback
		// 那么在执行查询语句的时候，就可以封装和指定的查询结果的处理逻辑
		// 2.封装在一个内部接口的匿名内部类对象中，传入JDBCHelper的方法
		// 在方法内部，可以回调定义的逻辑，处理查询结果
		// 并将查询结果，放入外部的变量中
		jdbcHelper.executeQuery(
				"select name,age from test_user where id=?", 
				new Object[]{5}, 
				new JDBCHelper.QueryCallback() {
					
					@Override
					public void process(ResultSet rs) throws Exception {
						if(rs.next()) {
							String name = rs.getString(1);
							int age = rs.getInt(2);
							
							testUser.put("name", name);
							testUser.put("age", age);
						}
					}

					
					
				});
		
		System.out.println(testUser.get("name") + ":" + testUser.get("age"));     
		
		// 测试批量执行SQL语句
		String sql = "insert into test_user(name,age) values(?,?)"; 
		
		List<Object[]> paramsList = new ArrayList<Object[]>();
		paramsList.add(new Object[]{"张三", 30});
		paramsList.add(new Object[]{"王五", 35});
		
		jdbcHelper.executeBatch(sql, paramsList);
	}

}
