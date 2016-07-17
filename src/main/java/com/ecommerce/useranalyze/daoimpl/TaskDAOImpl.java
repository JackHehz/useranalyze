/**
 * 
 *	TODO 实现TaskDAO，根据主键查询任务
 */
package com.ecommerce.useranalyze.daoimpl;

import java.sql.ResultSet;

import com.ecommerce.useranalyze.dao.TaskDAO;
import com.ecommerce.useranalyze.domain.Task;
import com.ecommerce.useranalyze.jdbchelper.JDBCHelper;

/**
 * @author hz
 *
 */
public class TaskDAOImpl implements TaskDAO{

	/**
	 * 根据主键查询任务
	 * @param taskid 主键
	 * @return 任务
	 */
	public Task findById(long taskid) {
		final Task task = new Task();
		
		String sql = "select * from task where task_id=?";
		Object[] params = new Object[]{taskid};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {
			
			@Override
			public void process(ResultSet rs) throws Exception {
				if(rs.next()) {
					long taskid = rs.getLong(1);
					String taskName = rs.getString(2);
					String createTime = rs.getString(3);
					String startTime = rs.getString(4);
					String finishTime = rs.getString(5);
					String taskType = rs.getString(6);
					String taskStatus = rs.getString(7);
					String taskParam = rs.getString(8);
					
					task.setTaskid(taskid);
					task.setTaskName(taskName); 
					task.setCreateTime(createTime); 
					task.setStartTime(startTime);
					task.setFinishTime(finishTime);
					task.setTaskType(taskType);  
					task.setTaskStatus(taskStatus);
					task.setTaskParam(taskParam);  
				}
			}
			
		});
		
		return task;
	}
}
