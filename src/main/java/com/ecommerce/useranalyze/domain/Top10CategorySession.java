/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.domain;

/**
 * @author hz
 *
 */
public class Top10CategorySession {

	private long taskid;
	private long categoryid;
	private String sessionid;
	private long clickCount;
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
	 * @return the categoryid
	 */
	public long getCategoryid() {
		return categoryid;
	}
	/**
	 * @param categoryid the categoryid to set
	 */
	public void setCategoryid(long categoryid) {
		this.categoryid = categoryid;
	}
	/**
	 * @return the sessionid
	 */
	public String getSessionid() {
		return sessionid;
	}
	/**
	 * @param sessionid the sessionid to set
	 */
	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}
	/**
	 * @return the clickCount
	 */
	public long getClickCount() {
		return clickCount;
	}
	/**
	 * @param clickCount the clickCount to set
	 */
	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}
	
}
