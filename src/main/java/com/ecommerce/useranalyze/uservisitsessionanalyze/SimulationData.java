/**
 * 
 *	TODO 获取模拟数据
 */
package com.ecommerce.useranalyze.uservisitsessionanalyze;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import com.ecommerce.useranalyze.confmanager.ConfigurationManager;
import com.ecommerce.useranalyze.contstants.Constants;
import com.ecommerce.useranalyze.util.SimulateData;

/**
 * @author hz
 *
 */
public class SimulationData {

	
	public static void SimulationData(JavaSparkContext sc, SQLContext sqlContext) {
		//生成模拟数据（只有本地模式，才会去生成模拟数据）
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			SimulateData.simulation(sc, sqlContext);
		}
	}
}
