/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.dao;

import com.ecommerce.useranalyze.domain.SessionAggrStat;

/**
 * @author hz
 *
 */
public interface SessionAggrStatDAO {

	/**
	 * 插入session聚合统计结果
	 * @param sessionAggrStat 
	 */
	void insert(SessionAggrStat sessionAggrStat);
}
