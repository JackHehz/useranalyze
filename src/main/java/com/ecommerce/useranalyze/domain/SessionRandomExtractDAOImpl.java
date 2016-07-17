/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.domain;

import com.ecommerce.useranalyze.dao.SessionRandomExtractDAO;
import com.ecommerce.useranalyze.jdbchelper.JDBCHelper;


/**
 * 随机抽取DAO实现
 * @author hz
 *
 */
public class SessionRandomExtractDAOImpl implements SessionRandomExtractDAO {

	/**
	 * 插入session随机抽取
	 * @param sessionAggrStat 
	 */
	public void insert(SessionRandomExtract sessionRandomExtract) {
		String sql = "insert into session_random_extract values(?,?,?,?,?)";
		
		Object[] params = new Object[]{sessionRandomExtract.getTaskid(),
				sessionRandomExtract.getSessionid(),
				sessionRandomExtract.getStartTime(),
				sessionRandomExtract.getSearchKeywords(),
				sessionRandomExtract.getClickCategoryIds()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
}
