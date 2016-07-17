/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.daoimpl;

import java.util.ArrayList;
import java.util.List;

import com.ecommerce.useranalyze.dao.SessionDetailDAO;
import com.ecommerce.useranalyze.domain.SessionDetail;
import com.ecommerce.useranalyze.jdbchelper.JDBCHelper;

/**
 * @author hz
 *
 */
public class SessionDetailDAOlmpl implements SessionDetailDAO{
	
	/**
	 * 插入一条session明细数据
	 * @param sessionDetail 
	 */
	public void insert(SessionDetail sessionDetail) {
		String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";  
		
		Object[] params = new Object[]{sessionDetail.getTaskid(),
				sessionDetail.getUserid(),
				sessionDetail.getSessionid(),
				sessionDetail.getPageid(),
				sessionDetail.getActionTime(),
				sessionDetail.getSearchKeyword(),
				sessionDetail.getClickCategoryId(),
				sessionDetail.getClickProductId(),
				sessionDetail.getOrderCategoryIds(),
				sessionDetail.getOrderProductIds(),
				sessionDetail.getPayCategoryIds(),
				sessionDetail.getPayProductIds()};  
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
	
	/**
	 * 批量插入session明细数据
	 * @param sessionDetails
	 */
	public void insertBatch(List<SessionDetail> sessionDetails) {
		String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";  
		
		List<Object[]> paramsList = new ArrayList<Object[]>();
		for(SessionDetail sessionDetail : sessionDetails) {
			Object[] params = new Object[]{sessionDetail.getTaskid(),
					sessionDetail.getUserid(),
					sessionDetail.getSessionid(),
					sessionDetail.getPageid(),
					sessionDetail.getActionTime(),
					sessionDetail.getSearchKeyword(),
					sessionDetail.getClickCategoryId(),
					sessionDetail.getClickProductId(),
					sessionDetail.getOrderCategoryIds(),
					sessionDetail.getOrderProductIds(),
					sessionDetail.getPayCategoryIds(),
					sessionDetail.getPayProductIds()};  
			paramsList.add(params);
		}
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(sql, paramsList);
	}

}
