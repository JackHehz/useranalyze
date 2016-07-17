/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.uservisitsessionanalyze;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.ecommerce.useranalyze.contstants.Constants;
import com.ecommerce.useranalyze.dao.SessionDetailDAO;
import com.ecommerce.useranalyze.dao.Top10CategorySessionDAO;
import com.ecommerce.useranalyze.dao.factory.DAOFactory;
import com.ecommerce.useranalyze.domain.SessionDetail;
import com.ecommerce.useranalyze.domain.Top10CategorySession;
import com.ecommerce.useranalyze.util.StringUtils;

import scala.Tuple2;
import tachyon.client.UfsUtils;

/**
 * @author hz
 *
 */
public class SortTop10Session {
	
	public static void getTop10Session(JavaSparkContext sc,
			final Long taskid,
			JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD,
			List<Tuple2<CategorySortKey, String>> top10CategoryList)
	{
		/**
		 * 第一步：获取符合条件的session访问过的所有品类
		 */
		
		// 获取符合条件的session的访问明细
		JavaPairRDD<String, Row> sessionid2detailRDD = filteredSessionid2AggrInfoRDD
				.join(sessionid2actionRDD).mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple2) throws Exception {
						return new Tuple2<String, Row>(tuple2._1, tuple2._2._2);
					}
				});
		//将top10的热门品类生成一份RDD
		JavaPairRDD<Long, Long> top10CategoryIdRDD ;
		List<Tuple2<Long, Long>> top10CategoryIdList = 
				new ArrayList<Tuple2<Long,Long>>();
		for(Tuple2<CategorySortKey, String> category:top10CategoryList){
			Long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString
					(category._2, "\\|", Constants.FIELD_CATEGORY_ID));
			top10CategoryIdList.add(new Tuple2<Long, Long>(categoryid,categoryid));
		}
		top10CategoryIdRDD =sc.parallelizePairs(top10CategoryIdList);
		
		/**
		 * 第二步：计算top10品类被各session点击的次数
		 */
		
		JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD = sessionid2detailRDD.groupByKey();
		
		//计算每一个品类被各个session点击的次数,即各个session点击每个品类的次数
		JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailsRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, Long, String>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
						String sessionid = tuple2._1;
						Iterator<Row> iterator = tuple2._2.iterator();
						Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();
						// 计算出该session，对每个品类的点击次数
						while(iterator.hasNext())
						{
							Row row = iterator.next();
							
							if(row.get(6)!=null)
							{
								long categoryid = row.getLong(6);
								Long count = categoryCountMap.get(categoryid);
								
								if(count == null) {
									count = 0L;
								}
								
								count++;
								
								categoryCountMap.put(categoryid, count);
							}
						}
						// 返回结果，<categoryid,sessionid,count>格式
						List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();
						for(Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
							long categoryid = categoryCountEntry.getKey();
							long count = categoryCountEntry.getValue();
							String value = sessionid + "," + count;
							list.add(new Tuple2<Long, String>(categoryid, value));  
						}
						return list;
					}
					
				});
		
		// 使用join获取到to10热门品类，被各个session点击的次数
		
		JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD
				.join(categoryid2sessionCountRDD)
				.mapToPair(new PairFunction<Tuple2<Long,Tuple2<Long,String>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
						return new Tuple2<Long, String>(tuple._1, tuple._2._2);						
					}
				});
		
		/**
		 * 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户
		 */
		JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD = 
				top10CategorySessionCountRDD.groupByKey();
		
		JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<Long,Iterable<String>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String, String>> call(
							Tuple2<Long, Iterable<String>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						Iterator<String> iterator = tuple._2.iterator();
						
						// 定义取topn的排序数组
						String[] top10Sessions = new String[10];  
						
						while(iterator.hasNext()) {
							String sessionCount = iterator.next();
							if(sessionCount==null)
								continue;
							long count = Long.valueOf(sessionCount.split(",")[1]);  
							
							// 遍历排序数组
							for(int i = 0; i < top10Sessions.length; i++) {
								// 如果当前i位，没有数据，那么直接将i位数据赋值为当前sessionCount
								if(top10Sessions[i] == null) {
									top10Sessions[i] = sessionCount;
									break;
								} else {
									long _count = Long.valueOf(top10Sessions[i].split(",")[1]);  
									
									// 如果sessionCount比i位的sessionCount要大
									if(count > _count) {
										// 从排序数组最后一位开始，到i位，所有数据往后挪一位
										for(int j = 9; j > i; j--) {
											top10Sessions[j] = top10Sessions[j - 1];
										}
										// 将i位赋值为sessionCount
										top10Sessions[i] = sessionCount;
										break;
									}
									
									// 比较小，继续外层for循环
								}
							}
						}
						
						// 将数据写入MySQL表
						List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
						
						for(String sessionCount : top10Sessions) {
							String sessionid = sessionCount.split(",")[0];
							long count = Long.valueOf(sessionCount.split(",")[1]);  
							
							// 将top10 session插入MySQL表
							Top10CategorySession top10Session = new Top10CategorySession();
							top10Session.setTaskid(taskid);  
							top10Session.setCategoryid(categoryid);  
							top10Session.setSessionid(sessionid);  
							top10Session.setClickCount(count);  
							
							Top10CategorySessionDAO top10SessionDAO = DAOFactory.getTop10CategorySessionDAO();
							top10SessionDAO.insert(top10Session);  
							
							// 放入list
							list.add(new Tuple2<String, String>(sessionid, sessionid));
						}
						
						return list;
					}
					
				});
		
		/**
		 * 第四步：获取top10活跃session的明细数据，并写入MySQL
		 */
		JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
				top10SessionRDD.join(sessionid2detailRDD);  
//		sessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {  
//			
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
//				Row row = tuple._2._2;
//				
//				SessionDetail sessionDetail = new SessionDetail();
//				sessionDetail.setTaskid(taskid);  
//				sessionDetail.setUserid(row.getLong(1));  
//				sessionDetail.setSessionid(row.getString(2));  
//				sessionDetail.setPageid(row.getLong(3));  
//				sessionDetail.setActionTime(row.getString(4));
//				sessionDetail.setSearchKeyword(row.getString(5));  
//				sessionDetail.setClickCategoryId(row.getLong(6));  
//				sessionDetail.setClickProductId(row.getLong(7));   
//				sessionDetail.setOrderCategoryIds(row.getString(8));  
//				sessionDetail.setOrderProductIds(row.getString(9));  
//				sessionDetail.setPayCategoryIds(row.getString(10)); 
//				sessionDetail.setPayProductIds(row.getString(11));  
//				
//				SessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
//				sessionDetailDAO.insertBatch(sessionDetail);  
//			}
//		});
		sessionDetailRDD.foreachPartition(
				new VoidFunction<Iterator<Tuple2<String,Tuple2<String,Row>>>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> iterator) throws Exception {
						while(iterator.hasNext())
						{
							Tuple2<String,Tuple2<String, Row>> tuple2 = iterator.next();
							Row row = tuple2._2._2;
							
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
					}
		});
	}

}
