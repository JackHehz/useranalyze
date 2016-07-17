/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.pagesanalyze;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;

import com.alibaba.fastjson.JSONObject;
import com.ecommerce.useranalyze.contstants.Constants;
import com.ecommerce.useranalyze.dao.PageSplitConvertRateDAO;
import com.ecommerce.useranalyze.dao.factory.DAOFactory;
import com.ecommerce.useranalyze.domain.PageSplitConvertRate;
import com.ecommerce.useranalyze.util.DateUtils;
import com.ecommerce.useranalyze.util.NumberUtils;
import com.ecommerce.useranalyze.util.ParamUtils;

import groovy.util.MapEntry;
import scala.Tuple2;



/**
 * 计算页面转换率
 * @author hz
 *
 */
public class GetPageOneStepConvertRate {
	public static JavaPairRDD<String, Row> getSessionid2actionRDD(JavaRDD<Row> actionRDD)
	{
		return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				String sessionid = row.getString(2);
				return new Tuple2<String, Row>(sessionid, row);
			}
		});
	}
	
	public static JavaPairRDD<String, Integer> generateAndMatchPageSplit(
			JavaSparkContext sc,
			JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD,
			JSONObject taskParam){
		String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		System.out.println(taskParam);
		System.out.println(targetPageFlow);
		final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);
		
		return sessionid2actionsRDD.flatMapToPair( 
				new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, String, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple) 
							throws Exception {
						//定义返回List
						List<Tuple2<String,Integer>> list = 
								new ArrayList<Tuple2<String, Integer>>();
						Iterator<Row> iterator = tuple._2.iterator();
						//System.out.println(targetPageFlowBroadcast.value());
						String[] targetPages = targetPageFlowBroadcast.value().split(",");
						List<Row> rows = new ArrayList<Row>();
						while(iterator.hasNext()){
							rows.add(iterator.next());
						}
						Collections.sort(rows, new Comparator<Row>() {
							
							@Override
							public int compare(Row o1, Row o2) {
								String actionTime1 = o1.getString(4);
								String actionTime2 = o2.getString(4);
								
								Date date1 = DateUtils.parseTime(actionTime1);
								Date date2 = DateUtils.parseTime(actionTime2);
								
								return (int)(date1.getTime() - date2.getTime());
							}
							
						});
						
						Long lastPageId = null;
						for(Row row:rows)
						{
							long pageid = row.getLong(3);
							if(lastPageId == null)
							{
								lastPageId = pageid;
								continue;
							}
							String pageSplit = lastPageId + "_" + pageid;
							for(int i = 1; i<targetPages.length;i++)
							{
								String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
								
								if(pageSplit.equals(targetPageSplit))
								{
									list.add(new Tuple2<String, Integer>(pageSplit,1));
									break;
								}
							}
							lastPageId = pageid;
						}
						
						return list;
					}
		});
	}
	
	
	//计算页面流初始页面的pv
	public static long getStartPagePv(JSONObject taskParam, 
			JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD){
		String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		final long  startPageId = Long.valueOf(targetPageFlow.split(",")[0]);
		
		JavaRDD<Long> startPageRDD = sessionid2actionsRDD.flatMap( 
				new FlatMapFunction<Tuple2<String,Iterable<Row>>, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
						List<Long> list = new ArrayList<Long>();
						Iterator<Row> rows = tuple._2.iterator();
						while(rows.hasNext())
						{
							Row row = rows.next();
							Long pageId = row.getLong(3);
							if(pageId == startPageId)
							{
								list.add(pageId);
							}
							
						}
						return list;
					}
		});
		return startPageRDD.count();
	}
	
	public static Map<String, Double> computePageSplitCovertRate(
			JSONObject taskParam,
			Map<String, Object>pageSplitPvMap,
			long startPagePv){
		Map<String,Double> pageConverRateMap =  new HashMap<String,Double>();
		String [] targetPages = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
				.split(",");
		long lastPageSplitPv = 0L;
		
		for(int i=1;i<targetPages.length;i++)
		{
			String targetPageSplit = targetPages[i-1]+"_"+targetPages[i];
			long targetPageSplitPv = Long.valueOf(String.valueOf(pageSplitPvMap.get(targetPageSplit)));
			double convertRate =0.0;
			
			if(i == 1)
			{
				convertRate = NumberUtils.formatDouble(
						(double)targetPageSplitPv/(double)startPagePv, 2);
			}
			else{
				convertRate= NumberUtils.formatDouble(
						(double)targetPageSplitPv/(double)lastPageSplitPv, 2);
			}
			pageConverRateMap.put(targetPageSplit, convertRate);
			lastPageSplitPv = targetPageSplitPv;
		}
		
			return pageConverRateMap;
	}
	
	public static void persistConvertRate(long taskid,
			Map<String, Double>convertRateMap)
	{
		StringBuffer buffer = new StringBuffer("");
		
		for(Map.Entry<String, Double> convertRateEntry : convertRateMap.entrySet())
		{
			String pageSplit = convertRateEntry.getKey();
			double convertRate = convertRateEntry.getValue();
			buffer.append(pageSplit+"="+convertRate+"|");
		}
		String converRates =  buffer.toString();
		converRates = converRates.substring(0,converRates.length()-1);
		
		PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
		pageSplitConvertRate.setTaskid(taskid);
		pageSplitConvertRate.setConvertRate(converRates);
		
		PageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getPageSplitConverRateDAP();
		pageSplitConvertRateDAO.insert(pageSplitConvertRate);
	}

}
