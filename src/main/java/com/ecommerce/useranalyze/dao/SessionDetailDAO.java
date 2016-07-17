/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.dao;

import java.util.List;

import com.ecommerce.useranalyze.domain.SessionDetail;

/**
 * @author hz
 *
 */
public interface SessionDetailDAO {
	
	/**
	 * 插入一条session明细数据
	 * @param sessionDetail 
	 */
	void insert(SessionDetail sessionDetail);
	void insertBatch(List<SessionDetail> sessionDetail);
}
