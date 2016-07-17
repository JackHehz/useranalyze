/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.pagesanalyze;

import java.util.Date;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONObject;
import com.ecommerce.useranalyze.contstants.Constants;
import com.ecommerce.useranalyze.dao.TaskDAO;
import com.ecommerce.useranalyze.dao.factory.DAOFactory;
import com.ecommerce.useranalyze.domain.Task;
import com.ecommerce.useranalyze.uservisitsessionanalyze.SimulationData;
import com.ecommerce.useranalyze.util.ParamUtils;
import com.ecommerce.useranalyze.util.SparkUtils;

import scala.tools.nsc.backend.icode.analysis.TypeFlowAnalysis.MethodTFA.Gen;

/**
 * @author hz
 *
 */
public class PageOneStepConvertRateMain {
	public static void main(String[] args) {
		//获取Spark上下文
		SparkConf sparkConf = new SparkConf()
							.setAppName(Constants.SPARK_APP_NAME_PAGE);
		//判断Spark运行模式
		SparkUtils.setMaster(sparkConf);
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
		System.out.println(sqlContext);
		//生成模拟数据
		SimulationData.SimulationData(sc, sqlContext);
		//获取任务
		long taskid = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PAGE);
		TaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(taskid);
		if(task == null) {
			System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");  
			return;
		}
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		//获取指定范围内的用户行为数据
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
		JavaPairRDD<String,Row> sessionId2actionRDD = 
				GetPageOneStepConvertRate.getSessionid2actionRDD(actionRDD);
		sessionId2actionRDD = sessionId2actionRDD.cache();
		JavaPairRDD<String, Iterable<Row>> sessionId2actionsRDD = sessionId2actionRDD.groupByKey();
		System.out.println(sessionId2actionsRDD);
		
		//每个session的单跳页面切片的生成，以及页面流的匹配
		JavaPairRDD<String, Integer> pageSplitRDD = GetPageOneStepConvertRate.generateAndMatchPageSplit
				(sc, sessionId2actionsRDD, taskParam);
		Map<String, Object> pageSplitPvMap = pageSplitRDD.countByKey();
		//得到页面流第一个页面的PV
		long startPagePv = GetPageOneStepConvertRate.getStartPagePv(taskParam, sessionId2actionsRDD);
		
		Map<String, Double> convertRateMap = GetPageOneStepConvertRate.computePageSplitCovertRate
				(taskParam, pageSplitPvMap, startPagePv);
		//持久化，写入mysql数据库
		GetPageOneStepConvertRate.persistConvertRate(taskid, convertRateMap);
	}

}
