/**
 * 
 *	TODO 测试TaskDAO
 */
package com.ecommerce.useranalyze.test;

import com.ecommerce.useranalyze.dao.TaskDAO;
import com.ecommerce.useranalyze.dao.factory.DAOFactory;
import com.ecommerce.useranalyze.domain.Task;

/**
 * @author hz
 *
 */
public class TaskDAOTest {

	/**hz 
	 * @param args
	 */
	public static void main(String[] args) {
		TaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(1);
		System.out.println(task.getTaskName());

	}

}
