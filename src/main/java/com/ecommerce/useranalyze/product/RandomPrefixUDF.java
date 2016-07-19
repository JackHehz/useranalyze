/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.product;

import java.util.Random;

import org.apache.spark.sql.api.java.UDF2;

/**
 * @author hz
 *
 */
public class RandomPrefixUDF implements UDF2<String, Integer, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public String call(String val, Integer num) throws Exception {
		Random random = new Random();
		int randNum = random.nextInt(10);
		return randNum + "_" + val;
	}
	
}
