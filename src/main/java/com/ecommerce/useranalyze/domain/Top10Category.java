/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.domain;

/**
 * @author hz
 *
 */
public class Top10Category {

	private long taskid;
	private long categoryid;
	private long clickCount;
	private long orderCount;
	private long payCount;
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
	/**
	 * @return the orderCount
	 */
	public long getOrderCount() {
		return orderCount;
	}
	/**
	 * @param orderCount the orderCount to set
	 */
	public void setOrderCount(long orderCount) {
		this.orderCount = orderCount;
	}
	/**
	 * @return the payCount
	 */
	public long getPayCount() {
		return payCount;
	}
	/**
	 * @param payCount the payCount to set
	 */
	public void setPayCount(long payCount) {
		this.payCount = payCount;
	}
	
}
