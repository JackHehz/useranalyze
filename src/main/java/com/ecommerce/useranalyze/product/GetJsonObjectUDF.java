/**
 * 
 *	TODO	自定义 UDF获取JSONObject中的字段
 */
package com.ecommerce.useranalyze.product;

import org.apache.spark.sql.api.java.UDF2;

import com.alibaba.fastjson.JSONObject;

/**
 * @author hz
 *
 */
public class GetJsonObjectUDF implements UDF2<String, String, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String call(String json, String field) throws Exception {
		try {
			JSONObject jsonObject = JSONObject.parseObject(json);
			return jsonObject.getString(field);
		} catch (Exception e) {
			e.printStackTrace();  
		}
		return null;
	}

}
