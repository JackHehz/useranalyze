package com.ecommerce.useranalyze.product;

import java.util.Arrays;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * @author hz
 *
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction{
private static final long serialVersionUID = -2510776241322950505L;
	
	// 指定输入数据的字段与类型
	private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("cityInfo", DataTypes.StringType, true)));  
	// 指定缓冲数据的字段与类型
	private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)));  
	// 指定返回类型
	private DataType dataType = DataTypes.StringType;
	// 指定是否是确定性的
	private boolean deterministic = true;
	
	@Override
	public StructType inputSchema() {
		return inputSchema;
	}
	
	@Override
	public StructType bufferSchema() {
		return bufferSchema;
	}

	@Override
	public DataType dataType() {
		return dataType;
	}

	@Override
	public boolean deterministic() {
		return deterministic;
	}
	
	/**
	 * 初始化
	 */
	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, "");  
	}
	
	/**
	 * 更新,	  一个一个地将组内的字段值传递进来
	 * 实现拼接的逻辑
	 */
	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		// 缓冲中的已经拼接过的城市信息串
		String bufferCityInfo = buffer.getString(0);
		// 传递进来的某个城市信息
		String cityInfo = input.getString(0);
		
		// 实现去重的逻辑
		// 判断：之前没有拼接过某个城市信息，那么这里才可以接下去拼接新的城市信息
		if(!bufferCityInfo.contains(cityInfo)) {
			if("".equals(bufferCityInfo)) {
				bufferCityInfo += cityInfo;
			} else {
				
				bufferCityInfo += "," + cityInfo;
			}
			
			buffer.update(0, bufferCityInfo);  
		}
	}
	
	/**
	 * 合并， update操作， 如果一个分组内的数据，会分布在多个节点上处理
	 * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
	 */
	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		String bufferCityInfo1 = buffer1.getString(0);
		String bufferCityInfo2 = buffer2.getString(0);
		
		for(String cityInfo : bufferCityInfo2.split(",")) {
			if(!bufferCityInfo1.contains(cityInfo)) {
				if("".equals(bufferCityInfo1)) {
					bufferCityInfo1 += cityInfo;
				} else {
					bufferCityInfo1 += "," + cityInfo;
				}
 			}
		}
		
		buffer1.update(0, bufferCityInfo1);  
	}
	
	@Override
	public Object evaluate(Row row) {  
		return row.getString(0);  
	}

}
