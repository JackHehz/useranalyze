/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.dao;

import java.util.List;

import com.ecommerce.useranalyze.domain.AdBlackList;
import com.ibeifeng.sparkproject.domain.AdBlacklist;

/**广告黑名单DAO接口
 * @author hz
 *
 */
public interface AdBlackListDAO {

	/**
	 * 批量插入广告黑名单用户
	 * @param adBlacklists
	 */
	void insertBatch(List<AdBlackList> adBlacklists);
	
	/**
	 * 查询所有广告黑名单用户
	 * @return
	 */
	List<AdBlackList> findAll();
	
}
