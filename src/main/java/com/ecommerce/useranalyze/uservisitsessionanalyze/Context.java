/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.uservisitsessionanalyze;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import com.ecommerce.useranalyze.confmanager.ConfigurationManager;
import com.ecommerce.useranalyze.contstants.Constants;

/**
 * @author hz
 *
 */
public class Context {
	
	/**
	 * 获取SQLContext
	 * 如果是在本地测试环境的话，那么就生成SQLContext对象
	 * 如果是在生产环境运行的话，那么就生成HiveContext对象
	 * @param sc SparkContext
	 * @return SQLContext
	 */

	public static SQLContext getSQLContext(SparkContext sc) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc);
		}
	}
}
