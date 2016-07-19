/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.daoimpl;

import java.util.ArrayList;
import java.util.List;

import com.ecommerce.useranalyze.dao.AreaTop5ProductDAO;
import com.ecommerce.useranalyze.domain.AreaTop5Product;
import com.ecommerce.useranalyze.jdbchelper.JDBCHelper;


/**各区域top5热门商品DAO实现类
 * @author hz
 *
 */
public class AreaTop5ProductDAOImpl implements AreaTop5ProductDAO {

	@Override
	public void insertBatch(List<AreaTop5Product> areaTopsProducts) {
		String sql = "INSERT INTO area_top5_product VALUES(?,?,?,?,?,?,?,?)";
		
		List<Object[]> paramsList = new ArrayList<Object[]>();
		
		for(AreaTop5Product areaTop3Product : areaTopsProducts) {
			Object[] params = new Object[8];
			
			params[0] = areaTop3Product.getTaskid();
			params[1] = areaTop3Product.getArea();
			params[2] = areaTop3Product.getAreaLevel();
			params[3] = areaTop3Product.getProductid();
			params[4] = areaTop3Product.getCityInfos();
			params[5] = areaTop3Product.getClickCount();
			params[6] = areaTop3Product.getProductName();
			params[7] = areaTop3Product.getProductStatus();
			
			paramsList.add(params);
		}
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(sql, paramsList);
	}
}
