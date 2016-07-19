/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.advertisement;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import com.ecommerce.useranalyze.dao.AdBlackListDAO;
import com.ecommerce.useranalyze.dao.AdUserClickCountDAO;
import com.ecommerce.useranalyze.dao.factory.DAOFactory;
import com.ecommerce.useranalyze.domain.AdBlackList;
import com.ecommerce.useranalyze.domain.AdUserClickCount;
import com.ecommerce.useranalyze.util.DateUtils;

import scala.Tuple2;

/**
 * @author hz
 *
 */
public class GenerateDynamicBlackList {

	public static void generateDynamicBlacklist(
			JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
		
				// 一条一条的实时日志
				// timestamp province city userid adid
				// 某个时间点 某个省份 某个城市 某个用户 某个广告
				
				// 计算出每5个秒内的数据中，每天每个用户每个广告的点击量
				
				// 通过对原始实时日志的处理
				// 将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式
				JavaPairDStream<String, Long> dailyUserAdClickDStream = filteredAdRealTimeLogDStream.mapToPair(
						
						new PairFunction<Tuple2<String,String>, String, Long>() {

							private static final long serialVersionUID = 1L;
				
							@Override
							public Tuple2<String, Long> call(Tuple2<String, String> tuple)
									throws Exception {
								// 从tuple中获取到每一条原始的实时日志
								String log = tuple._2;
								String[] logSplited = log.split(" "); 
								
								// 提取出日期（yyyyMMdd）、userid、adid
								String timestamp = logSplited[0];
								Date date = new Date(Long.valueOf(timestamp));
								String datekey = DateUtils.formatDateKey(date);
								
								long userid = Long.valueOf(logSplited[3]);
								long adid = Long.valueOf(logSplited[4]); 
								
								// 拼接key
								String key = datekey + "_" + userid + "_" + adid;
								
								return new Tuple2<String, Long>(key, 1L);  
							}
							
						});
				
				// 针对处理后的日志格式，执行reduceByKey算子即可
				// （每个batch中）每天每个用户对每个广告的点击量
				JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(
						
						new Function2<Long, Long, Long>() {
					
							private static final long serialVersionUID = 1L;
				
							@Override
							public Long call(Long v1, Long v2) throws Exception {
								return v1 + v2;
							}
							
						});
				
				//每个5s的batch中，当天每个用户对每支广告的点击次数
				// <yyyyMMdd_userid_adid, clickCount>
				dailyUserAdClickCountDStream.foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {
				
					private static final long serialVersionUID = 1L;

					@Override
					public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
							rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {

								private static final long serialVersionUID = 1L;

								@Override
								public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
									// 对每个分区的数据就去获取一次连接对象
									// 每次都是从连接池中获取，而不是每次都创建
									List<AdUserClickCount> adUserClickCounts = new ArrayList<AdUserClickCount>();
									while(iterator.hasNext()) {
										Tuple2<String, Long> tuple = iterator.next();
										
										String[] keySplited = tuple._1.split("_");
										String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
										// yyyy-MM-dd
										long userid = Long.valueOf(keySplited[1]);
										long adid = Long.valueOf(keySplited[2]);  
										long clickCount = tuple._2;
										
										AdUserClickCount adUserClickCount = new AdUserClickCount();
										adUserClickCount.setDate(date);
										adUserClickCount.setUserid(userid); 
										adUserClickCount.setAdid(adid);  
										adUserClickCount.setClickCount(clickCount); 
										
										adUserClickCounts.add(adUserClickCount);
									}
									AdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
									adUserClickCountDAO.updateBatch(adUserClickCounts);

								}
							});
						return null;
					}
				});
			
						// 遍历每个batch中的所有记录，对每条记录都要去查询一下，这一天这个用户对这个广告的累计点击量是多少
						// 查询出来的结果，如果是某个用户某天对某个广告的点击量>=100，
						// 那么就判定这个用户就是黑名单用户，就写入mysql的表中，持久化
					JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(
						
						new Function<Tuple2<String,Long>, Boolean>() {

							private static final long serialVersionUID = 1L;

							@Override
							public Boolean call(Tuple2<String, Long> tuple)
									throws Exception {
								String key = tuple._1;
								String[] keySplited = key.split("_");  
								
								// yyyyMMdd -> yyyy-MM-dd
								String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));  
								long userid = Long.valueOf(keySplited[1]);  
								long adid = Long.valueOf(keySplited[2]);  
								
								// 从mysql中查询指定日期指定用户对指定广告的点击量
								AdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
								int clickCount = adUserClickCountDAO.findClickCountByMultiKey(
										date, userid, adid);
								//如果点击量大于等于100，返回true
								if(clickCount >= 100) {
									return true;
								}
								
								// 反之，如果点击量小于100,返回false
								return false;
							}
							
						});
					
					//通过对dstream执行操作，对其中的rdd中的userid进行全局的去重
					JavaDStream<Long> blacklistUseridDStream = blacklistDStream.map(
							
							new Function<Tuple2<String,Long>, Long>() {

								private static final long serialVersionUID = 1L;
					
								@Override
								public Long call(Tuple2<String, Long> tuple) throws Exception {
									String key = tuple._1;
									String[] keySplited = key.split("_");  
									Long userid = Long.valueOf(keySplited[1]);  
									return userid;
								}
								
							});
					
					JavaDStream<Long> distinctBlacklistUseridDStream = blacklistUseridDStream.transform(
							
							new Function<JavaRDD<Long>, JavaRDD<Long>>() {

								private static final long serialVersionUID = 1L;
					
								@Override
								public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
									return rdd.distinct();
								}
								
							});
					
					distinctBlacklistUseridDStream.foreachRDD(new Function<JavaRDD<Long>, Void>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Void call(JavaRDD<Long> rdd) throws Exception {
							
							rdd.foreachPartition(new VoidFunction<Iterator<Long>>() {

								private static final long serialVersionUID = 1L;

								@Override
								public void call(Iterator<Long> iterator) throws Exception {
									List<AdBlackList> adBlacklists = new ArrayList<AdBlackList>();
									
									while(iterator.hasNext()) {
										long userid = iterator.next();
										
										AdBlackList adBlacklist = new AdBlackList();
										adBlacklist.setUserid(userid); 
										
										adBlacklists.add(adBlacklist);
									}
									
									AdBlackListDAO adBlacklistDAO = DAOFactory.getAdBlackListDAO();
									adBlacklistDAO.insertBatch(adBlacklists); 
									
									
									
									// 1、计算出每个batch中的每天每个用户对每个广告的点击量，并持久化到mysql中
									
									// 2、依据上述计算出来的数据，对每个batch中的按date、userid、adid聚合的数据
									// 都要遍历一遍，查询一下，对应的累计的点击次数，如果超过了100，那么就认定为黑名单
									// 然后对黑名单用户进行去重，去重后，将黑名单用户，持久化插入到mysql中
									
									
									// 3、基于上述计算出来的动态黑名单，在最一开始，就对每个batch中的点击行为
									// 根据动态黑名单进行过滤
									// 把黑名单中的用户的点击行为，直接过滤掉
									
								
									
								}
								
							});
							
							return null;
						}
						
					});
	}
}
