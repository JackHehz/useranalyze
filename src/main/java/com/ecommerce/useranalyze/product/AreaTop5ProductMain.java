/**
 * 
 *	TODO 各个区域top5热门商品统计
 */
package com.ecommerce.useranalyze.product;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;

import com.alibaba.fastjson.JSONObject;
import com.ecommerce.useranalyze.contstants.Constants;
import com.ecommerce.useranalyze.dao.TaskDAO;
import com.ecommerce.useranalyze.dao.factory.DAOFactory;
import com.ecommerce.useranalyze.domain.Task;
import com.ecommerce.useranalyze.uservisitsessionanalyze.SimulationData;
import com.ecommerce.useranalyze.util.ParamUtils;
import com.ecommerce.useranalyze.util.SparkUtils;

/**
 * @author hz
 *
 */
public class AreaTop5ProductMain {

	/**hz 
	 * @param args
	 */
	public static void main(String[] args) {
		
		// 创建SparkConf
				SparkConf conf = new SparkConf()
						.setAppName("AreaTop5ProductMain");
				SparkUtils.setMaster(conf); 
				
				// 构建Spark上下文
				JavaSparkContext sc = new JavaSparkContext(conf);
				SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
//				sqlContext.setConf("spark.sql.shuffle.partitions", "1000");
//				sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "20971520");
				
				// 注册自定义函数
				sqlContext.udf().register("concat_long_string", 
						new ConcatLongStringUDF(), DataTypes.StringType);
				sqlContext.udf().register("get_json_object", 
						new GetJsonObjectUDF(), DataTypes.StringType);
				sqlContext.udf().register("group_concat_distinct", 
						new GroupConcatDistinctUDAF());
				sqlContext.udf().register("random_prefix", 
						new RandomPrefixUDF(), DataTypes.StringType);
				sqlContext.udf().register("remove_random_prefix", 
						new RemoveRandomPrefixUDF(), DataTypes.StringType);
				
				//准备模拟数据
				SimulationData.SimulationData(sc, sqlContext);
				
				// 获取命令行传入的taskid，查询对应的任务参数
				TaskDAO taskDAO = DAOFactory.getTaskDAO();
				
				long taskid = ParamUtils.getTaskIdFromArgs(args, 
						Constants.SPARK_LOCAL_TASKID_PRODUCT);
				Task task = taskDAO.findById(taskid);
				
				JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
				String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
				String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
				
				// 查询用户指定日期范围内的点击行为数据
				JavaPairRDD<Long,Row> clickActionRDD = ClickRDDs.getClickActionRDDByDate(
						sqlContext, startDate, endDate);
				
				// 从MySQL中查询城市信息
				JavaPairRDD<Long, Row> cityInfoRDD = ClickRDDs.getcityid2CityInfoRDD(sqlContext);
				
				// 生成点击商品基础信息临时表
				
				ClickRDDs.generateTempClickProductBasicTable(sqlContext, 
						clickActionRDD, cityInfoRDD); 
				

				// 生成各区域各商品点击次数的临时表
				ClickRDDs.generateTempAreaPrdocutClickCountTable(sqlContext);
				
				// 生成包含完整商品信息的各区域各商品点击次数的临时表
				ClickRDDs.generateTempAreaFullProductClickCountTable(sqlContext);  
				
				// 使用开窗函数获取各个区域内点击次数排名前5的热门商品
				JavaRDD<Row> areaTop5ProductRDD = ClickRDDs.getAreaTop5ProductRDD(sqlContext);
				
				// 用批量插入的方式，一次性插入mysql即可
				List<Row> rows = areaTop5ProductRDD.collect();
				ClickRDDs.persistAreaTop5Product(taskid, rows);
				
				sc.close();

	}

}
