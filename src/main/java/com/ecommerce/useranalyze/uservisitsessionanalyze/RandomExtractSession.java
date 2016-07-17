/**
 * 
 *	TODO 随机抽取session
 */
package com.ecommerce.useranalyze.uservisitsessionanalyze;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;

import com.ecommerce.useranalyze.contstants.Constants;
import com.ecommerce.useranalyze.dao.SessionDetailDAO;
import com.ecommerce.useranalyze.dao.SessionRandomExtractDAO;
import com.ecommerce.useranalyze.dao.factory.DAOFactory;
import com.ecommerce.useranalyze.domain.SessionDetail;
import com.ecommerce.useranalyze.domain.SessionRandomExtract;
import com.ecommerce.useranalyze.util.DateUtils;
import com.ecommerce.useranalyze.util.StringUtils;
import it.unimi.dsi.fastutil.ints.*;


import scala.Tuple2;

/**
 * @author hz
 *
 */
public class RandomExtractSession {

	public static void randomExtractSession (JavaSparkContext sc,
			final long taskid,
			JavaPairRDD<String, String> sessionid2AggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD){
		//第一步，计算出每天每小时的session数量，获取<yyyy-MM-dd_HH,session>格式的RDD
		JavaPairRDD<String, String> time2sessionRDD =  sessionid2AggrInfoRDD.mapToPair(
				new PairFunction<Tuple2<String,String>, String, String>() {
	
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
						String aggrInfo = tuple2._2;
						String startTime = StringUtils.getFieldFromConcatString(
								aggrInfo, "\\|", Constants.FIELD_START_TIME);
						String dateHour = DateUtils.getDateHour(startTime);
						return new Tuple2<String, String>(dateHour, aggrInfo);
					}
				} ) ;
		//得到每天每小时的session数量
		Map<String, Object> countMap = time2sessionRDD.countByKey();
		
		// 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
		
				// 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
				Map<String, Map<String, Long>> dateHourCountMap = 
						new HashMap<String, Map<String, Long>>();
				
				for(Map.Entry<String, Object> countEntry : countMap.entrySet())
				{
					String dateHour = countEntry.getKey();
					String date = dateHour.split("_")[0];
					String hour = dateHour.split("_")[1];  
					
					long count = Long.valueOf(String.valueOf(countEntry.getValue()));  
					
					Map<String, Long> hourCountMap = dateHourCountMap.get(date);
					if(hourCountMap == null) {
						hourCountMap = new HashMap<String, Long>();
						dateHourCountMap.put(date, hourCountMap);
					}
					
					hourCountMap.put(hour, count);

				}
				
				// 开始实现我们的按时间比例随机抽取算法
				
				// 总共要抽取100个session，先按照天数，进行平分
				int extractNumberPerDay = 100 / dateHourCountMap.size();
				
				// <date,<hour,(3,5,20,102)>>  
				//将dateHourExtractMap转换成fastutil,并做成一个广播变量以及Kryo序列化优化性能
				Map<String, Map<String, List<Integer>>> dateHourExtractMap = 
						new HashMap<String, Map<String, List<Integer>>>();
				
				Random random = new Random();
				
				for(Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
					String date = dateHourCountEntry.getKey();
					Map<String, Long> hourCountMap = dateHourCountEntry.getValue();
					
					// 计算出这一天的session总数
					long sessionCount = 0L;
					for(long hourCount : hourCountMap.values()) {
						sessionCount += hourCount;
					}
					
					Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
					if(hourExtractMap == null) {
						hourExtractMap = new HashMap<String, List<Integer>>();
						dateHourExtractMap.put(date, hourExtractMap);
					}
					
					// 遍历每个小时
					for(Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
						String hour = hourCountEntry.getKey();
						long count = hourCountEntry.getValue();
						
						// 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
						// 就可以计算出，当前小时需要抽取的session数量
						int hourExtractNumber = (int)(((double)count / (double)sessionCount) 
								* extractNumberPerDay);
						if(hourExtractNumber > count) {
							hourExtractNumber = (int) count;
						}
						
						// 先获取当前小时的存放随机数的list
						List<Integer> extractIndexList = hourExtractMap.get(hour);
						if(extractIndexList == null) {
							extractIndexList = new ArrayList<Integer>();
							hourExtractMap.put(hour, extractIndexList);
						}
						
						// 生成上面计算出来的数量的随机数
						for(int i = 0; i < hourExtractNumber; i++) {
							int extractIndex = random.nextInt((int) count);
							while(extractIndexList.contains(extractIndex)) {
								extractIndex = random.nextInt((int) count);
							}
							extractIndexList.add(extractIndex);
						}
					}
				}
				
				/**
				 * fastutil的使用，很简单，比如List<Integer>的list，对应到fastutil，就是IntList
				 */
				Map<String, Map<String, IntList>> fastutilDateHourExtractMap = 
						new HashMap<String, Map<String, IntList>>();
				
				
				
				for(Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry : 
						dateHourExtractMap.entrySet()) {
					String date = dateHourExtractEntry.getKey();
					Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();
					
					Map<String, IntList> fastutilHourExtractMap = new HashMap<String, IntList>();
					
					for(Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
						String hour = hourExtractEntry.getKey();
						List<Integer> extractList = hourExtractEntry.getValue();
						
						IntArrayList fastutilExtractList = new IntArrayList();
						
						for(int i = 0; i < extractList.size(); i++) {
							
							fastutilExtractList.add(extractList.get(i));  
						}
						
						fastutilHourExtractMap.put(hour, fastutilExtractList);
					}
					
					fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
				}
				
				//将fastutilDateHourExtractMap做成广播变量
				final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast =
						sc.broadcast(fastutilDateHourExtractMap);
				
				/**
				 * 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
				 */
				
				// 执行groupByKey算子，得到<dateHour,(session aggrInfo)>  
				JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionRDD.groupByKey();
				
				// 我们用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的数据
				// 然后呢，会遍历每天每小时的session
				// 如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上
				// 那么抽取该session，直接写入MySQL的random_extract_session表
				// 将抽取出来的session id返回回来，形成一个新的JavaRDD<String>
				// 然后最后一步，是用抽取出来的sessionid，去join它们的访问行为明细数据，写入session表
				JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(
						
						new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {

							private static final long serialVersionUID = 1L;

							@Override
							public Iterable<Tuple2<String, String>> call(
									Tuple2<String, Iterable<String>> tuple)
									throws Exception {
								List<Tuple2<String, String>> extractSessionids = 
										new ArrayList<Tuple2<String, String>>();
								
								String dateHour = tuple._1;
								String date = dateHour.split("_")[0];
								String hour = dateHour.split("_")[1];
								Iterator<String> iterator = tuple._2.iterator();
								
								/**
								 * 使用广播变量的时候
								 * 直接调用广播变量（Broadcast类型）的value() / getValue() 
								 * 可以获取到之前封装的广播变量
								 */
								Map<String, Map<String, IntList>> dateHourExtractMap =
										dateHourExtractMapBroadcast.value();
								List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);  
								
								SessionRandomExtractDAO sessionRandomExtractDAO = 
										DAOFactory.getSessionRandomExtractDAO();
								
								int index = 0;
								while(iterator.hasNext()) {
									String sessionAggrInfo = iterator.next();
									
									if(extractIndexList.contains(index)) {
										String sessionid = StringUtils.getFieldFromConcatString(
												sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
										
										// 将数据写入MySQL
										SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
										sessionRandomExtract.setTaskid(taskid);  
										sessionRandomExtract.setSessionid(sessionid);  
										sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
												sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));  
										sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
												sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
										sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
												sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
										
										sessionRandomExtractDAO.insert(sessionRandomExtract);  
										
										// 将sessionid加入list
										extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));  
									}
									
									index++;
								}
								
								return extractSessionids;
							}
							
						});
				
				
				/**
				 * 第四步：获取抽取出来的session的明细数据
				 */
				JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
						extractSessionidsRDD.join(sessionid2actionRDD);
				extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {  
					
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
						Row row = tuple._2._2;
						
						SessionDetail sessionDetail = new SessionDetail();
						sessionDetail.setTaskid(taskid);  
						sessionDetail.setUserid(row.getLong(1));  
						sessionDetail.setSessionid(row.getString(2));  
						sessionDetail.setPageid(row.getLong(3));  
						sessionDetail.setActionTime(row.getString(4));
						sessionDetail.setSearchKeyword(row.getString(5));  
						sessionDetail.setClickCategoryId(row.getLong(6));  
						sessionDetail.setClickProductId(row.getLong(7));   
						sessionDetail.setOrderCategoryIds(row.getString(8));  
						sessionDetail.setOrderProductIds(row.getString(9));  
						sessionDetail.setPayCategoryIds(row.getString(10)); 
						sessionDetail.setPayProductIds(row.getString(11));  
						
						SessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
						sessionDetailDAO.insert(sessionDetail);  
					}
				});
		}
		

}