/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.advertisement;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.ecommerce.useranalyze.confmanager.ConfigurationManager;
import com.ecommerce.useranalyze.contstants.Constants;
import com.ecommerce.useranalyze.dao.AdBlackListDAO;
import com.ecommerce.useranalyze.dao.AdUserClickCountDAO;
import com.ecommerce.useranalyze.dao.factory.DAOFactory;
import com.ecommerce.useranalyze.domain.AdBlackList;
import com.ecommerce.useranalyze.domain.AdUserClickCount;
import com.ecommerce.useranalyze.util.DateUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import scala.Tuple2;



/**
 * @author hz
 *
 */
public class AdClickRealTimeStatMain {

	public static void main(String[] args) {
		// 构建Spark Streaming上下文
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("AdClickRealTimeStatMain");
		
		// 每隔5秒钟，spark streaming作业就会收集最近5秒内的数据源接收过来的数据
		JavaStreamingContext jssc = new JavaStreamingContext(
				conf, Durations.seconds(5));  
		
		// 主要放置连接的kafka集群的地址（broker集群的地址列表）
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LIST, 
				ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
		
		// 构建topic set
		String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
		String[] kafkaTopicsSplited = kafkaTopics.split(",");  
		
		Set<String> topics = new HashSet<String>();
		for(String kafkaTopic : kafkaTopicsSplited) {
			topics.add(kafkaTopic);
		}
		
		// 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
		// 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
		JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
				jssc, 
				String.class, 
				String.class, 
				StringDecoder.class, 
				StringDecoder.class, 
				kafkaParams, 
				topics);
		
		// 根据动态黑名单进行数据过滤
		JavaPairDStream<String, String> filteredAdRealTimeLogDStream = 
						FilterByBlackList.filterByBlacklist(adRealTimeLogDStream);
		// 生成动态黑名单
		GenerateDynamicBlackList.generateDynamicBlacklist(filteredAdRealTimeLogDStream);
		
		// 构建完spark streaming上下文之后，进行上下文的启动、等待执行结束、关闭
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
