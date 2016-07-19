package com.ecommerce.useranalyze.dao.factory;

import com.ecommerce.useranalyze.dao.AdBlackListDAO;
import com.ecommerce.useranalyze.dao.AdUserClickCountDAO;
import com.ecommerce.useranalyze.dao.AreaTop5ProductDAO;
import com.ecommerce.useranalyze.dao.PageSplitConvertRateDAO;
import com.ecommerce.useranalyze.dao.SessionAggrStatDAO;
import com.ecommerce.useranalyze.dao.SessionDetailDAO;
import com.ecommerce.useranalyze.dao.SessionRandomExtractDAO;
import com.ecommerce.useranalyze.dao.TaskDAO;
import com.ecommerce.useranalyze.dao.Top10CategoryDAO;
import com.ecommerce.useranalyze.dao.Top10CategorySessionDAO;
import com.ecommerce.useranalyze.daoimpl.AdBlackListDAOImpl;
import com.ecommerce.useranalyze.daoimpl.AdUserClickCountDAOImpl;
import com.ecommerce.useranalyze.daoimpl.AreaTop5ProductDAOImpl;
import com.ecommerce.useranalyze.daoimpl.PageSplitConvertRateDAOImpl;
import com.ecommerce.useranalyze.daoimpl.SessionAggrStatDAOImpl;
import com.ecommerce.useranalyze.daoimpl.SessionDetailDAOlmpl;
import com.ecommerce.useranalyze.daoimpl.TaskDAOImpl;
import com.ecommerce.useranalyze.daoimpl.Top10CategoryDAOImpl;
import com.ecommerce.useranalyze.daoimpl.Top10CategorySessionDAOImpl;
import com.ecommerce.useranalyze.domain.SessionRandomExtractDAOImpl;

/**
 * DAO工厂类
 * @author hz
 *
 */
public class DAOFactory {

	/**
	 * 获取任务管理DAO
	 * @return DAO
	 */
	public static TaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}

	/**hz 
	 * @return
	 */
	public static SessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}
	
	public static SessionDetailDAO getSessionDetailDAO(){
		return new SessionDetailDAOlmpl();
	}
	
	public static SessionRandomExtractDAO getSessionRandomExtractDAO(){
		return new SessionRandomExtractDAOImpl();
	}
	
	public static Top10CategoryDAO geTop10CategoryDAO(){
		return new Top10CategoryDAOImpl();
	}
	
	public static Top10CategorySessionDAO getTop10CategorySessionDAO(){
		return new Top10CategorySessionDAOImpl();
	}
	public static PageSplitConvertRateDAO getPageSplitConverRateDAO(){
		return new PageSplitConvertRateDAOImpl();
	}
	public static AreaTop5ProductDAO getTop5ProductDAO(){
		return new AreaTop5ProductDAOImpl();
	}

	/**hz 
	 * @return
	 */
	public static AdUserClickCountDAO getAdUserClickCountDAO() {
		
		return new AdUserClickCountDAOImpl();
	}

	/**hz 
	 * @return
	 */
	public static AdBlackListDAO getAdBlackListDAO() {
		
		return new AdBlackListDAOImpl();
	}
}
