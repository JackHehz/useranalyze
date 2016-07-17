/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.dao;

import com.ecommerce.useranalyze.domain.Task;

/**
 * @author hz
 *
 */
public interface TaskDAO {
 
	/**
	 * 根据主键查询任务
	 * @param taskid 主键
	 * @return 任务
	 */
	Task findById(long taskid);
	
}
