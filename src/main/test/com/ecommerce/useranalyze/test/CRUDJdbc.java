/**
 * 
 *	JDBC增删改查示例类
 */
package com.ecommerce.useranalyze.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author hz
 *
 */
public class CRUDJdbc {
	public static void main(String[] args) {
		insert();
		update();
		delete();
		select();
		preparedStatement();
	}
		/**
		 * 测试插入数据
		 */
		private static void insert() {
			Connection conn = null;
			Statement stmt = null;
			
			try {
				Class.forName("com.mysql.jdbc.Driver");  
				
				// 获取数据库的连接
				// 使用DriverManager.getConnection()方法获取针对数据库的连接
				// 需要给方法传入三个参数，包括url、user、password
				// 其中url就是有特定格式的数据库连接串，包括“主协议:子协议://主机名:端口号//数据库”
				conn = DriverManager.getConnection(
						"jdbc:mysql://localhost:3306/spark", 
						"root", 
						"jackhe");  
				
				// 基于数据库连接Connection对象，创建SQL语句执行句柄，Statement对象
				// Statement对象，就是用来基于底层的Connection代表的数据库连接
				// 通过Statement对象，向MySQL数据库发送SQL语句
				// 从而实现通过发送的SQL语句来执行增删改查等逻辑
				stmt = conn.createStatement();
				
				// 然后就可以基于Statement对象，来执行insert SQL语句
				// 插入一条数据
				// Statement.executeUpdate()方法，就可以用来执行insert、update、delete语句
				// 返回类型是个int值，也就是SQL语句影响的行数
				String sql = "insert into test_user(name,age) values('李四',26)";  
				int rtn = stmt.executeUpdate(sql);    
				
				System.out.println("SQL语句影响了【" + rtn + "】行。");  
			} catch (Exception e) {
				e.printStackTrace();  
			} finally {
				// 释放数据库连接
				try {
					if(stmt != null) {
						stmt.close();
					} 
					if(conn != null) {
						conn.close();  
					}
				} catch (Exception e2) {
					e2.printStackTrace();
				}
			}
		}
		
		/**
		 * 测试更新数据
		 */
		private static void update(){
			Connection conn = null;
			Statement stmt = null;
			try {
				Class.forName("com.mysql.jdbc.Driver");
				conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "jackhe");
				stmt = conn.createStatement();
				String sql = "Update test_user set(age)values(28)where name ='李四'";
				stmt.executeUpdate(sql);
			} catch (Exception e) {
				e.printStackTrace();
			}finally {
				try {
					if(stmt!=null)
					{
						stmt.close();
					}
					if(conn!=null)
					{
						conn.close();
					}
				} catch (Exception e2) {
					e2.printStackTrace();
				}
			}
		}
		/**
		 * 测试删除数据
		 */
		private static void delete() {
			Connection conn = null;
			Statement stmt = null;
			
			try {
				Class.forName("com.mysql.jdbc.Driver");  
				
				conn = DriverManager.getConnection(
						"jdbc:mysql://localhost:3306/spark_project", 
						"root", 
						"root"); 
				stmt = conn.createStatement();
				
				String sql = "delete from test_user where name='李四'";
				int rtn = stmt.executeUpdate(sql);
				
				System.out.println("SQL语句影响了【" + rtn + "】行。");  
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					if(stmt != null) {
						stmt.close();
					} 
					if(conn != null) {
						conn.close();
					}
				} catch (Exception e2) {
					e2.printStackTrace(); 
				}
			}
		}
		
		/**
		 * 测试查询数据
		 */
		private static void select() {
			Connection conn = null;
			Statement stmt = null;
			// 对于select查询语句，需要定义ResultSet
			// ResultSet就代表了，你的select语句查询出来的数据
			// 需要通过ResutSet对象，来遍历你查询出来的每一行数据，然后对数据进行保存或者处理
			ResultSet rs = null;
			
			try {
				Class.forName("com.mysql.jdbc.Driver");  
				
				conn = DriverManager.getConnection(
						"jdbc:mysql://localhost:3306/spark_project", 
						"root", 
						"root"); 
				stmt = conn.createStatement();
				
				String sql = "select * from test_user";
				rs = stmt.executeQuery(sql);
				
				// 获取到ResultSet以后，就需要对其进行遍历，然后获取查询出来的每一条数据
				while(rs.next()) {
					int id = rs.getInt(1);
					String name = rs.getString(2);
					int age = rs.getInt(3);
					System.out.println("id=" + id + ", name=" + name + ", age=" + age);    
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					if(stmt != null) {
						stmt.close();
					} 
					if(conn != null) {
						conn.close();  
					}
				} catch (Exception e2) {
					e2.printStackTrace();
				}
			}
		}
		
		/**
		 * 测试PreparedStatement
		 */
		private static void preparedStatement() {
			Connection conn = null;
			PreparedStatement pstmt = null;
			
			try {
				Class.forName("com.mysql.jdbc.Driver");  
				
				conn = DriverManager.getConnection(
						"jdbc:mysql://localhost:3306/spark_project?characterEncoding=utf8", 
						"root", 
						"jakche");  
				
				// 1.SQL语句中，值所在的地方，都用?表示
				String sql = "insert into test_user(name,age) values(?,?)";
				
				pstmt = conn.prepareStatement(sql);
				
				// 2。必须调用PreparedStatement的setX()系列方法，对指定的占位符设置实际的值
				pstmt.setString(1, "李四");  
				pstmt.setInt(2, 26);  
				
				// 3.执行SQL语句时，直接使用executeUpdate()即可，不用传入任何参数
				int rtn = pstmt.executeUpdate();    
				
				System.out.println("SQL语句影响了【" + rtn + "】行。");  
			} catch (Exception e) {
				e.printStackTrace();  
			} finally {
				try {
					if(pstmt != null) {
						pstmt.close();
					} 
					if(conn != null) {
						conn.close();  
					}
				} catch (Exception e2) {
					e2.printStackTrace();
				}
			}
		}
}


