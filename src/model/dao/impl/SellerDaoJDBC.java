package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao {
	
	private Connection conn;
	
	public SellerDaoJDBC (Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Seller seller) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void update(Seller seller) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteById(Integer id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Seller findById(Integer id) {
		
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			
			st = conn.prepareStatement("SELECT `seller`.*, `department`.`name` AS DepName "
					+ "	FROM `seller` "
					+ "INNER JOIN `department` "
					+ "ON `seller`.`department_id` = `department`.`id` "
					+ "WHERE `seller`.`id` = ? ");
			
			st.setInt(1, id);
			
			rs = st.executeQuery();
			
			if(rs.next()) {
				Department department = instantiateDepartment(rs);
				
				Seller seller = instantiateSeller(rs, department);
				return seller;
					
			}
			return null;
		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

	private Seller instantiateSeller(ResultSet rs, Department department) throws SQLException {
		
		Seller seller = new Seller();
		seller.setId(rs.getInt("id"));
		seller.setName(rs.getString("name"));
		seller.setEmail(rs.getString("email"));
		seller.setBirthDate(rs.getDate("birth_date"));
		seller.setBaseSalary(rs.getDouble("base_salary"));
		seller.setDepartment(department);
		return seller;
	}

	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		
		Department department = new Department();
		department.setId(rs.getInt("department_id"));
		department.setName(rs.getString("DepName"));
		return department;
	}

	@Override
	public List<Seller> findAll() {
		// TODO Auto-generated method stub
		return null;
	}

}
