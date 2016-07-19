/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.daoimpl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.ecommerce.useranalyze.dao.AdBlackListDAO;
import com.ecommerce.useranalyze.domain.AdBlackList;
import com.ecommerce.useranalyze.jdbchelper.JDBCHelper;



/**
 * @author hz
 *
 */
public class AdBlackListDAOImpl implements AdBlackListDAO {

	
	@Override
	public void insertBatch(List<AdBlackList> adBlacklists) {
		String sql = "INSERT INTO ad_blacklist VALUES(?)";
		
		List<Object[]> paramsList = new ArrayList<Object[]>();
		
		for(AdBlackList adBlacklist : adBlacklists) {
			Object[] params = new Object[]{adBlacklist.getUserid()};
			paramsList.add(params);
		}
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(sql, paramsList);
		
	}
	
	/**
	 * 查询所有广告黑名单用户
	 * @return
	 */
	public List<AdBlackList> findAll() {
		String sql = "SELECT * FROM ad_blacklist"; 
		
		final List<AdBlackList> adBlacklists = new ArrayList<AdBlackList>();
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		jdbcHelper.executeQuery(sql, null, new JDBCHelper.QueryCallback() {
			
			@Override
			public void process(ResultSet rs) throws Exception {
				while(rs.next()) {
					long userid = Long.valueOf(String.valueOf(rs.getInt(1)));  
					
					AdBlackList adBlacklist = new AdBlackList();
					adBlacklist.setUserid(userid);  
					
					adBlacklists.add(adBlacklist);
				}
			}
			
		});
		
		return adBlacklists;
	}

}
