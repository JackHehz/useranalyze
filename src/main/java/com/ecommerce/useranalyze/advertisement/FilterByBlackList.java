/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.advertisement;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;

import com.ecommerce.useranalyze.dao.AdBlackListDAO;
import com.ecommerce.useranalyze.dao.factory.DAOFactory;
import com.ecommerce.useranalyze.domain.AdBlackList;
import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * @author hz
 *
 */
public class FilterByBlackList {

	public static JavaPairDStream<String, String> filterByBlacklist(
			JavaPairInputDStream<String, String> adRealTimeLogDStream) {
		// 根据mysql中的动态黑名单，进行实时的黑名单过滤
		// 使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD）
		
		JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(
				
				new Function<JavaPairRDD<String,String>, JavaPairRDD<String,String>>() {
		
					private static final long serialVersionUID = 1L;
		
					@SuppressWarnings("resource")
					@Override
					public JavaPairRDD<String, String> call(
							JavaPairRDD<String, String> rdd) throws Exception {
						
						// 首先，从mysql中查询所有黑名单用户，将其转换为一个rdd
						AdBlackListDAO adBlacklistDAO = DAOFactory.getAdBlackListDAO();
						List<AdBlackList> adBlacklists = adBlacklistDAO.findAll();
						
						List<Tuple2<Long, Boolean>> tuples = new ArrayList<Tuple2<Long, Boolean>>();
						
						for(AdBlackList adBlacklist : adBlacklists) {
							tuples.add(new Tuple2<Long, Boolean>(adBlacklist.getUserid(), true));  
						}
						
						JavaSparkContext sc = new JavaSparkContext(rdd.context());
						JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);
						
						// 将原始数据rdd映射成<userid, tuple2<string, string>>    
						JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(new PairFunction<Tuple2<String,String>, Long, Tuple2<String, String>>() {
		
							private static final long serialVersionUID = 1L;
		
							@Override
							public Tuple2<Long, Tuple2<String, String>> call(  
									Tuple2<String, String> tuple)
									throws Exception {
								String log = tuple._2;
								String[] logSplited = log.split(" "); 
								long userid = Long.valueOf(logSplited[3]);
								return new Tuple2<Long, Tuple2<String, String>>(userid, tuple);  
							}
							
						});
						
						// 将原始日志数据rdd，与黑名单rdd，进行左外连接
				
						JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD = 
								mappedRDD.leftOuterJoin(blacklistRDD);
						
						JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(
								
								new Function<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, Boolean>() {
		
									private static final long serialVersionUID = 1L;
		
									@Override
									public Boolean call(
											Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple)
											throws Exception {
										Optional<Boolean> optional = tuple._2._2;
										
										// 如果这个值存在，那么说明原始日志中的userid，join到了某个黑名单用户
										if(optional.isPresent() && optional.get()) {
											return false;
										}
										
										return true;
									}
									
								});
						
						JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(
								
								new PairFunction<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, String, String>() {
		
									private static final long serialVersionUID = 1L;
		
									@Override
									public Tuple2<String, String> call(
											Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple)
											throws Exception {
										return tuple._2._1;
									}
									
								});
						
						return resultRDD;
					}
					
				});
		
		return filteredAdRealTimeLogDStream;
	}
	
}
