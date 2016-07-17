/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.daoimpl;

import com.ecommerce.useranalyze.dao.Top10CategorySessionDAO;
import com.ecommerce.useranalyze.domain.Top10CategorySession;
import com.ecommerce.useranalyze.jdbchelper.JDBCHelper;


/**top10的品类的top10的session点击次数实现类
 * @author hz
 *
 */
public class Top10CategorySessionDAOImpl implements Top10CategorySessionDAO {
	
	@Override
	public void insert(Top10CategorySession top10Session) {
		String sql = "insert into top10_category_session values(?,?,?,?)"; 
		
		Object[] params = new Object[]{top10Session.getTaskid(),
				top10Session.getCategoryid(),
				top10Session.getSessionid(),
				top10Session.getClickCount()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);

	}
}