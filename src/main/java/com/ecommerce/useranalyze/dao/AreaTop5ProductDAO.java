/**
 * 
 *	TODO
 */
package com.ecommerce.useranalyze.dao;

import java.util.List;
import com.ecommerce.useranalyze.domain.AreaTop5Product;

/**各区域top5热门商品DAO接口
 * @author hz
 *
 */
public interface AreaTop5ProductDAO {
	void insertBatch(List<AreaTop5Product> areaTopsProducts);

}
