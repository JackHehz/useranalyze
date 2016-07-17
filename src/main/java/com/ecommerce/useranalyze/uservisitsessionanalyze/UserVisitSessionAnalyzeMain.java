/**
 * 
 *	TODO 用户访问session分析main函数
 */
package com.ecommerce.useranalyze.uservisitsessionanalyze;

import java.util.Date;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import com.alibaba.fastjson.JSONObject;
import com.ecommerce.useranalyze.contstants.Constants;
import com.ecommerce.useranalyze.dao.TaskDAO;
import com.ecommerce.useranalyze.dao.factory.DAOFactory;
import com.ecommerce.useranalyze.domain.Task;
import com.ecommerce.useranalyze.util.ParamUtils;
import com.ecommerce.useranalyze.util.SparkUtils;

import it.unimi.dsi.fastutil.ints.IntList;
import scala.Tuple2;

/**
 * @author hz
 *
 */
public class UserVisitSessionAnalyzeMain {

	/**hz 
	 * @param args
	 */
	public static void main(String[] args) {
		args = new String[]{"1"};  
		
		// 构建Spark上下文
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_SESSION)
				.set("spark.storage.memoryFraction", "0.5")  
				.set("spark.shuffle.consolidateFiles", "true")
				.set("spark.shuffle.file.buffer", "64")  
				.set("spark.shuffle.memoryFraction", "0.3")    
				.set("spark.reducer.maxSizeInFlight", "24")  
				.set("spark.shuffle.io.maxRetries", "60")  
				.set("spark.shuffle.io.retryWait", "60")   
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.registerKryoClasses(new Class[]{CategorySortKey.class,
				IntList.class});  
		SparkUtils.setMaster(conf);
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = Context.getSQLContext(sc.sc());
		
		// 生成模拟测试数据
		SimulationData.SimulationData(sc, sqlContext);
		
		// 创建需要使用的DAO组件
		TaskDAO taskDAO = DAOFactory.getTaskDAO();
		
		// 首先得查询出来指定的任务，并获取任务的查询参数
		long taskid = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_SESSION);
		Task task = taskDAO.findById(taskid);
		if(task == null) {
			System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");  
			return;
		}
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		// 如果要进行session粒度的数据聚合
		// 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
		JavaRDD<Row> actionRDD = UserAggregateBySession.getActionRDDByDateRange(sqlContext, taskParam);
		JavaPairRDD<String, Row> sessionid2actionRDD = UserAggregateBySession.getSessionid2ActionRDD(actionRDD);
		//持久化sessionid2actionRDD
		sessionid2actionRDD =sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());
		
		

		// 然后就可以获取到session粒度的数据，数据里面还包含了session对应的user的信息
		JavaPairRDD<String, String> sessionid2AggrInfoRDD = 
				UserAggregateBySession.aggregateBySession(sqlContext, sessionid2actionRDD);
		
		// 接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤,
		//同时进行过滤和统计
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
				"", new SessionAggrStatAccumulator());
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = 
				FilterSessionByParams.filterSession(sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
		//持久化filteredSessionid2AggrInfoRDD
		filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());
		//随机抽取session
		RandomExtractSession.randomExtractSession(sc,taskid,filteredSessionid2AggrInfoRDD,sessionid2actionRDD);
		
		// 计算出各个范围的session占比，并写入MySQL
				CalculateAndPersistAggrStat.calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),
						task.getTaskid());
		//获得top10热门品类		
		List<Tuple2<CategorySortKey, String>> top10Category = SortTop10Category.getTop10Category
					(task.getTaskid(),filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
		//获得top10热门品类的top10Session点击次数
		SortTop10Session.getTop10Session(sc, taskid, filteredSessionid2AggrInfoRDD, 
										sessionid2actionRDD, top10Category);
		// 关闭Spark上下文
		sc.close(); 
	}

}
