package application;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import db.DB;
import db.DbException;
import db.DbIntegrityException;

public class Program {

	public static void main(String[] args) {
		//testaConexao();
		//select();
		//insertDepto();
		//insertSeller();
		//update();
		//delete();
		transacao();
	}
	
	private static void transacao() {
		
		Connection conn = null;
		Statement st = null;
		
		String update1 = "UPDATE seller set BaseSalary = 2090 where DepartmentId = 1";
		String update2 = "UPDATE seller set BaseSalary = 3090 where DepartmentId = 2";
		
		try {
			conn = DB.getConnection();
			conn.setAutoCommit(false); // agora só vai concluir a transação quando der commit
			
			st = conn.createStatement();
			
			int rows1 = st.executeUpdate(update1);
			
			// quebra do programa - atualiza somente update1
			int x = 1;
			if (x < 2) {
				throw new SQLException("Fake error!");
			}
			//
			
			int rows2 = st.executeUpdate(update2);
			
			conn.commit(); // agora só vai concluir a transação quando der commit
			
			System.out.println("rows1: " + rows1);
			System.out.println("rows2: " + rows2);
			
		} catch (SQLException e) {
			// se der erro faz rollback da transação
			try {
				conn.rollback();
				throw new DbException("Transaction rolled back! Caused by: " + e.getMessage());
			} catch (SQLException e1) {
				throw new DbException("Erro trying to rollback! Caused by: " + e.getMessage());
			}
		
		
		} finally {
			DB.closeStatement(st);
			DB.closeConnection();
		}
	}

	
	
	private static void delete() {
		
		Connection conn = null;
		PreparedStatement st = null;
		
		String delete = "DELETE FROM department WHERE Id = ?";
		
		try {
			conn = DB.getConnection();
			st = conn.prepareStatement(delete);
			st.setInt(1, 2);
			
			int rowsAffected = st.executeUpdate();
			
			System.out.println("DONE! Rows affected: " + rowsAffected);
			
		} catch (SQLException e) {
			throw new DbIntegrityException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeConnection();
		}

		
	}
	
	private static void update() {
		Connection conn = null;
		PreparedStatement st = null;
		
		String update = "UPDATE seller SET BaseSalary = BaseSalary + ? WHERE (DepartmentId = ?)";
		
		try {
			conn = DB.getConnection();
			st = conn.prepareStatement(update);
			st.setDouble(1, 200.0);
			st.setInt(2, 2);
			
			int rowsAffected = st.executeUpdate();
			
			System.out.println("DONE! Rows affected: " + rowsAffected);
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DB.closeStatement(st);
			DB.closeConnection();
		}
		
	}
	
	

	private static void insertDepto() {
		Connection conn = null;
		PreparedStatement st = null;

		String sqlDept = ("INSERT INTO department (Name) VALUES ('D1'), ('D2')");

		try {
			conn = DB.getConnection();
			st = conn.prepareStatement(sqlDept, Statement.RETURN_GENERATED_KEYS); // Retorna o id gerado
			
			int rowsAffected = st.executeUpdate();
			
			if (rowsAffected > 0) {
				ResultSet rs = st.getGeneratedKeys();
				while (rs.next()) {
					int id = rs.getInt(1);
					System.out.println("DONE - Id: " + id);
				}
			} else {
				System.out.println("No row affected!");
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DB.closeStatement(st);
			DB.closeConnection();
		}
	}
	
	private static void insertSeller() {
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
		Connection conn = null;
		PreparedStatement st = null;

		String sqlSeller = "INSERT INTO seller"
				+ "(Name, Email, BirthDate, BaseSalary, DepartmentId)"
				+ "VALUES "
				+ "(?,?,?,?,?)";

		try {
			conn = DB.getConnection();
			st = conn.prepareStatement(sqlSeller, Statement.RETURN_GENERATED_KEYS); // Retorna o id gerado
			
			st.setString(1, "Carlos Purpurina");
			st.setString(2, "email@gmail.com");
			st.setDate(3, new java.sql.Date(sdf.parse("19/10/1979").getTime()));
			st.setDouble(4, 3000.0);
			st.setInt(5, 4);
			
			int rowsAffected = st.executeUpdate();
			
			if (rowsAffected > 0) {
				ResultSet rs = st.getGeneratedKeys();
				while (rs.next()) {
					int id = rs.getInt(1);
					System.out.println("DONE - Id: " + id);
				}
			} else {
				System.out.println("No row affected!");
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} finally {
			DB.closeStatement(st);
			DB.closeConnection();
		}
	}

	

	private static void select() {
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		
		try {
			conn = DB.getConnection();
			st = conn.createStatement();
			rs = st.executeQuery("select * from department");
			
			while(rs.next()) {
				System.out.println(rs.getInt("Id") + ", " + rs.getString("Name"));
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DB.closeResultSet(rs);
			DB.closeStatement(st);
			DB.closeConnection();
		}
	}

	private static void testaConexao() {
		Connection conn = DB.getConnection();
		DB.closeConnection();
	}

}
