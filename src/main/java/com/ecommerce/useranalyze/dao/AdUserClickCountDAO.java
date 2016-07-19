/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.dao;

import java.util.List;

import com.ecommerce.useranalyze.domain.AdUserClickCount;

/**用户广告点击量DAO
 * @author hz
 *
 */
public interface AdUserClickCountDAO {

	void updateBatch(List<AdUserClickCount> adUserClickCounts);

	/**hz 
	 * @param date
	 * @param userid
	 * @param adid
	 * @return
	 */
	int findClickCountByMultiKey(String date, long userid, long adid);
}
