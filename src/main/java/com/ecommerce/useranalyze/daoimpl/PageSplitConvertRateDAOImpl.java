package com.ecommerce.useranalyze.daoimpl;

import com.ecommerce.useranalyze.dao.PageSplitConvertRateDAO;
import com.ecommerce.useranalyze.domain.PageSplitConvertRate;
import com.ecommerce.useranalyze.jdbchelper.JDBCHelper;

/**
 * 页面切片转化率DAO实现类
 * @author hz
 *
 */
public class PageSplitConvertRateDAOImpl implements PageSplitConvertRateDAO {

	@Override
	public void insert(PageSplitConvertRate pageSplitConvertRate) {
		String sql = "insert into page_split_convert_rate values(?,?)";  
		Object[] params = new Object[]{pageSplitConvertRate.getTaskid(), 
				pageSplitConvertRate.getConvertRate()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
