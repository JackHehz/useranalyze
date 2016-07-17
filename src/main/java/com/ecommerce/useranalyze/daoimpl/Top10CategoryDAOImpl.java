/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.daoimpl;

import com.ecommerce.useranalyze.dao.Top10CategoryDAO;
import com.ecommerce.useranalyze.domain.Top10Category;
import com.ecommerce.useranalyze.jdbchelper.JDBCHelper;


/**
 * @author hz
 *
 */
public class Top10CategoryDAOImpl implements Top10CategoryDAO {
	@Override
	public void insert(Top10Category category) {
		String sql = "insert into top10_category values(?,?,?,?,?)";  
		
		Object[] params = new Object[]{category.getTaskid(),
				category.getCategoryid(),
				category.getClickCount(),
				category.getOrderCount(),
				category.getPayCount()};  
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);

	}
}
