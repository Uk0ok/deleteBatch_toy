import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DeleteRun {

	private int count = 0;
	private int succCount = 0;
	private int failCount = 0;

	private Connection conn = null;

	public void run() {
		long pTime = 0l;
		try {
			// load db class driver
			try {
				Class.forName(DeleteMain.DB_DRIVER);
			} catch (ClassNotFoundException e) {
				System.out.println("Failed to load db driver, " + e.getMessage());
			}
	
			long startTime = System.currentTimeMillis();
			// Select Work List
			ArrayList<String> eidList = select();
			if (eidList == null) {
				System.out.println("No Update target File");
				return;
			} else {
				updateStatus(eidList);
			}
			count = eidList.size();
			
			long endTime = System.currentTimeMillis();
			pTime = (endTime-startTime)/1000;
		}  catch (Exception ex) {
					System.out.println("Proc Call error, " + ex.getMessage());
				}finally {
					System.out.println("=============================================");
					System.out.println("TOTAL COUNT\t\t: " + count);
					System.out.println("SUCCESS COUNT\t\t: " + succCount);
					System.out.println("FAIL COUNT(DOWN)\t: " + failCount);
					System.out.println("TOTAL TIME \t\t: " + pTime + " sec");
					System.out.println("=============================================");				
				}
	}

	public Connection getConnection(String url, String id, String pwd) {
		try {
			conn = DriverManager.getConnection(url, id, pwd);
			conn.setAutoCommit(false);
			
		} catch (SQLException e) {
			System.out.println("DB Connection error : " + e.getMessage());
			return null;
		}
		return conn;
	}

	private ArrayList<String> select() {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		ArrayList<String> ecmList = new ArrayList<String>();

		try {
			conn = getConnection(DeleteMain.DB_URL, DeleteMain.DB_ID, DeleteMain.DB_PWD);
			pstmt = conn.prepareStatement(DeleteMain.SELECTQUERY);
			pstmt.setFetchSize(100);
			rs = pstmt.executeQuery();
			
			while(rs.next()) {
				// ecmList.add(rs.getString("ELEMENTID"));
				if(rs.getString("ELEMENTID") == null || rs.getString("ELEMENTID").equals("")) {
					System.out.println("[ERROR]there's no ELEMENTID");
					
					continue;
				}
				ecmList.add(rs.getString("ELEMENTID"));
			}
		} catch (SQLException e) {
			System.out.println("Select Update List Error, " + e.getMessage());			
		} finally {
			try { 
				if (rs != null) { rs.close(); }
				if (pstmt != null) { pstmt.close(); }
//				if (conn != null) { conn.close(); }
			} catch (SQLException e) {
				System.out.println("open cursor close error, " + e.getMessage());
			}
		}
		return ecmList;
	}

	public void updateStatus(List<String> ecmList) {
		// Connection conn = null;
		PreparedStatement pstmt = null;
		int ret = 0;
		
		try {
			conn = getConnection(DeleteMain.DB_URL, DeleteMain.DB_ID, DeleteMain.DB_PWD);
			pstmt = conn.prepareStatement(DeleteMain.UPDATEQUERY);
			for (String eid : ecmList) {
				pstmt.setString(1, eid);
				ret = pstmt.executeUpdate();
				succCount += 1;
				if (ret < 1) {
					System.out.println("[DBUpdate]ExcuteUpdate is Failed, " + eid);
					failCount += 1;
				}
			}
			conn.commit();
		} catch (SQLException e) {
			try {				
				if (conn != null) { conn.rollback(); }							
			} catch (SQLException e1) {
				System.out.println("Failed to rollback, " + e1.getMessage());
			}
			System.out.println("update status Error, " + e.getMessage());
		} finally {
			try {
				if (pstmt != null) { pstmt.close(); } 
//				if (conn != null) { conn.close(); }
			} catch (SQLException e) {
				System.out.println("open cursor close error, " + e.getMessage());
			}
		}
	}
	
	public void disconnDB() {
		try {
			if (conn != null) { conn.close(); }			
		} catch (Exception e) {
			System.out.println("Db DisConnect is failure");
		}
	}
}
