/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.domain;

import java.io.Serializable;

/**
 * @author hz
 *
 */
public class Task implements Serializable{


	private static final long serialVersionUID = 5205834512709570815L;

	
	private long taskid;
	private String taskName;
	private String createTime;
	private String startTime;
	private String finishTime;
	private String taskType;
	private String taskStatus;
	private String taskParam;
	
	/**
	 * @return the taskid
	 */
	public long getTaskid() {
		return taskid;
	}
	/**
	 * @param taskid the taskid to set
	 */
	public void setTaskid(long taskid) {
		this.taskid = taskid;
	}
	/**
	 * @return the taskName
	 */
	public String getTaskName() {
		return taskName;
	}
	/**
	 * @param taskName the taskName to set
	 */
	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}
	/**
	 * @return the createTime
	 */
	public String getCreateTime() {
		return createTime;
	}
	/**
	 * @param createTime the createTime to set
	 */
	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}
	/**
	 * @return the startTime
	 */
	public String getStartTime() {
		return startTime;
	}
	/**
	 * @param startTime the startTime to set
	 */
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	/**
	 * @return the finishTime
	 */
	public String getFinishTime() {
		return finishTime;
	}
	/**
	 * @param finishTime the finishTime to set
	 */
	public void setFinishTime(String finishTime) {
		this.finishTime = finishTime;
	}
	/**
	 * @return the taskType
	 */
	public String getTaskType() {
		return taskType;
	}
	/**
	 * @param taskType the taskType to set
	 */
	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}
	/**
	 * @return the taskStatus
	 */
	public String getTaskStatus() {
		return taskStatus;
	}
	/**
	 * @param taskStatus the taskStatus to set
	 */
	public void setTaskStatus(String taskStatus) {
		this.taskStatus = taskStatus;
	}
	/**
	 * @return the taskParam
	 */
	public String getTaskParam() {
		return taskParam;
	}
	/**
	 * @param taskParam the taskParam to set
	 */
	public void setTaskParam(String taskParam) {
		this.taskParam = taskParam;
	}
	
	
	
}
