/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.uservisitsessionanalyze;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;

import com.ecommerce.useranalyze.contstants.Constants;
import com.ecommerce.useranalyze.dao.Top10CategoryDAO;
import com.ecommerce.useranalyze.dao.factory.DAOFactory;
import com.ecommerce.useranalyze.domain.Top10Category;
import com.ecommerce.useranalyze.util.StringUtils;
import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * @author hz
 *
 */
public class SortTop10Category {
	
	public static List<Tuple2<CategorySortKey, String>> getTop10Category(Long taskid,
			JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD){
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
		// 获取session访问过的所有品类id
		// 访问过：指的是，点击过、下单过、支付过的品类
		
		JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(
					new PairFlatMapFunction<Tuple2<String,Row>,Long,Long>() {

						private static final long serialVersionUID = 1L;


						@Override
						public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
							Row row = tuple2._2;
							List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long,Long>>();
							Long clickCategoryId = row.getLong(6);
							if(clickCategoryId != null)
							{
								list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
							}
							
							String orderCategoryId = row.getString(8);
							if(orderCategoryId !=null)
							{
								list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), Long.valueOf(orderCategoryId)));
							}
							
							String payCategoryIds = row.getString(10);
							if(payCategoryIds !=null)
							{
								list.add(new Tuple2<Long,Long>(Long.valueOf(payCategoryIds),Long.valueOf(payCategoryIds)));
							}
							return list;
						}
					});
		
		//去重
		categoryidRDD =categoryidRDD.distinct();
		/**
		 * 第二步：计算各品类的点击、下单和支付的次数
		 */
		// 访问明细中，其中三种访问行为是：点击、下单和支付
				// 分别来计算各品类点击、下单和支付的次数，可以先对访问明细数据进行过滤
				// 分别过滤出点击、下单和支付行为，然后通过map、reduceByKey等算子来进行计算
				
				// 计算各个品类的点击次数
				JavaPairRDD<Long, Long> clickCategoryId2CountRDD = 
						getClickCategoryId2CountRDD(sessionid2detailRDD);
				// 计算各个品类的下单次数
				JavaPairRDD<Long, Long> orderCategoryId2CountRDD = 
						getOrderCategoryId2CountRDD(sessionid2detailRDD);
				// 计算各个品类的支付次数
				JavaPairRDD<Long, Long> payCategoryId2CountRDD = 
						getPayCategoryId2CountRDD(sessionid2detailRDD);
				
				
				/**
				 * 第三步：join各品类与它的点击、下单和支付的次数
				 * 
				 * categoryidRDD中，是包含了所有的符合条件的session，访问过的品类id
				 */
				
				JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(
						categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, 
						payCategoryId2CountRDD);
				
				
				/**
				 * 第四步：自定义二次排序key
				 */
				
				/**
				 * 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
				 */
				
				JavaPairRDD<CategorySortKey,String> sortKey2CountRDD = categoryid2countRDD.mapToPair(
						new PairFunction<Tuple2<Long,String>, CategorySortKey,String >() {

							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple2) throws Exception {
								String countInfo = tuple2._2;
								long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString
										(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
								long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
										countInfo, "\\|", Constants.FIELD_ORDER_COUNT));  
								long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
										countInfo, "\\|", Constants.FIELD_PAY_COUNT)); 
								CategorySortKey sortKey = new CategorySortKey(clickCount,
										orderCount, payCount);
								return new Tuple2<CategorySortKey, String>(sortKey, countInfo);
							}
						});
				JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = 
						sortKey2CountRDD.sortByKey(false);
				
				/**
				 * 第六步：用take(10)取出top10热门品类，并写入MySQL
				 */
				Top10CategoryDAO top10CategoryDAO = DAOFactory.geTop10CategoryDAO();
				
				
				List<Tuple2<CategorySortKey, String>> top10CategoryList = 
						sortedCategoryCountRDD.take(10);
				for(Tuple2<CategorySortKey, String> tuple :top10CategoryList)
				{
					String countInfo = tuple._2;
					long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
							countInfo, "\\|", Constants.FIELD_CATEGORY_ID));  
					long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
							countInfo, "\\|", Constants.FIELD_CLICK_COUNT));  
					long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
							countInfo, "\\|", Constants.FIELD_ORDER_COUNT));  
					long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
							countInfo, "\\|", Constants.FIELD_PAY_COUNT)); 
					
					Top10Category category = new Top10Category();
					category.setTaskid(taskid); 
					category.setCategoryid(categoryid); 
					category.setClickCount(clickCount);  
					category.setOrderCount(orderCount);
					category.setPayCount(payCount);
					
					top10CategoryDAO.insert(category);
				}
				return top10CategoryList;
		
		
	}
	
	/**
	 * 获取各品类点击次数RDD
	 * @param sessionid2detailRDD
	 * @return
	 */
	
	public static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(
			JavaPairRDD<String, Row>sessionid2detailRDD){
		//过滤空值
		JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(
					new Function<Tuple2<String,Row>, Boolean>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Boolean call(Tuple2<String, Row> tuple2) throws Exception {
							Row row = tuple2._2;
							return row.get(6) != null ? true : false;
						}
					}).coalesce(100);
		//映射次数
		JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
				new PairFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Long> call(Tuple2<String, Row> tuple2) throws Exception {
						long clickCategoryId = tuple2._2.getLong(6);
						return new Tuple2<Long, Long>(clickCategoryId, 1L);
					}
				});
		
		//统计次数
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(
				new Function2<Long, Long, Long>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						
						return v1+v2;
					}
					
				});
		//使用随机Key实现双重聚合
		//给每个Key打上一个随机数
		JavaPairRDD<String, Long> clickCategorysIdRDD = clickCategoryIdRDD.mapToPair(
				new PairFunction<Tuple2<Long,Long>, String, Long>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Long> call(Tuple2<Long, Long> tuple) throws Exception {
						Random random = new Random();
						int prefix = random.nextInt(10);
						return  new Tuple2<String, Long>(prefix+"_"+tuple._1, tuple._2);
					}
				});
		//执行第一轮局部聚合
		JavaPairRDD<String, Long> firstAggrRDD = clickCategorysIdRDD.reduceByKey(
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						
						return v1+v2;
					}
				});
		//去掉每个key的前缀
		JavaPairRDD<Long, Long> finalAggrRDD = firstAggrRDD.mapToPair(
				new PairFunction<Tuple2<String,Long>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Long> call(Tuple2<String, Long> tuple) throws Exception {
						long key = Long.valueOf(tuple._1.split("_")[1]);
						return new Tuple2<Long, Long>(key,tuple._2);
					}
				});
		//统计次数
		JavaPairRDD<Long, Long> clickCategorysId2CountRDD = finalAggrRDD.reduceByKey(
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						
						return v1+v2;
					}
				});
		
		return clickCategorysId2CountRDD;
		
		
		
	}
	
	/**
	 * 获取各品类的下单次数RDD
	 * @param sessionid2detailRDD
	 * @return
	 */
	
	public static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(
			JavaPairRDD<String, Row>sessionid2detailRDD){
		JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(
				new Function<Tuple2<String,Row>, Boolean>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Row> tuple2) throws Exception {
						Row row = tuple2._2;
						return row.getString(8)!=null ? true:false;
					}
				});
		JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
						Row row = tuple2._2;
						String orderCategoryIds = row.getString(8);
						String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						for(String orderCategoryId:orderCategoryIdsSplited)
						{
							list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),1L));
						}
						return list;
						
					}
				});
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						
						return v1+v2;
					}
				});
		return orderCategoryId2CountRDD;
	}
	
	/**
	 * 获取各个品类的支付次数RDD
	 * @param sessionid2detailRDD
	 * @return
	 */
	public static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(
				
				new Function<Tuple2<String,Row>, Boolean>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;  
						return row.getString(10) != null ? true : false;
					}
					
				});
		
		JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, Long>> call(
							Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						String payCategoryIds = row.getString(10);
						String[] payCategoryIdsSplited = payCategoryIds.split(",");  
						
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						
						for(String payCategoryId : payCategoryIdsSplited) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));  
						}
						
						return list;
					}
					
				});
		
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(
				
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
					
				});
		
		return payCategoryId2CountRDD;
	}
	public static JavaPairRDD<Long, String> joinCategoryAndData(
			JavaPairRDD<Long, Long> categoryidRDD,
			JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
			JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
			JavaPairRDD<Long, Long> payCategoryId2CountRDD){
		JavaPairRDD<Long, String> tmpJoinRDD = 
				categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD).mapToPair(
						new PairFunction<Tuple2<Long,Tuple2<Long,Optional<Long>>>, Long,String >() {

							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple2)
									throws Exception {
								long categoryid = tuple2._1;
								Optional<Long> option = tuple2._2._2;
								Long clickCount = 0L;
								if(option.isPresent())
								{
									clickCount = option.get();
								}
								String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" + 
										Constants.FIELD_CLICK_COUNT + "=" + clickCount;
								
								return new Tuple2<Long, String>(categoryid, value);
							}
								
							
						});
		
		tmpJoinRDD = tmpJoinRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple2) throws Exception {
						Long categoryid = tuple2._1;
						String value = tuple2._2._1;
						Optional<Long> optional =tuple2._2._2;
						long orderCount = 0L;
						
						if(optional.isPresent()) {
							orderCount = optional.get();
						}
						
						value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;
						return new Tuple2<Long, String>(categoryid, value) ;
					}
					});
		
		
		tmpJoinRDD = tmpJoinRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(
				
				new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						String value = tuple._2._1;
						
						Optional<Long> optional = tuple._2._2;
						long payCount = 0L;
						
						if(optional.isPresent()) {
							payCount = optional.get();
						}
						
						value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;  
						
						return new Tuple2<Long, String>(categoryid, value);  
					}
				
				});
		return tmpJoinRDD;
				}
		
}
