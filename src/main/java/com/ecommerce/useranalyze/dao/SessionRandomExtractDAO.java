/**
 * 
 *	TODO 
 */
package com.ecommerce.useranalyze.dao;

import com.ecommerce.useranalyze.domain.SessionRandomExtract;

/**
 * @author hz
 *
 */
public interface SessionRandomExtractDAO {
	
	/**
	 * 插入session随机抽取
	 * @param sessionAggrStat 
	 */
	void insert(SessionRandomExtract sessionRandomExtract);
}
