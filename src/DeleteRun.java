import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.log4j.Logger;

public class DeleteRun {

	private static Logger log = Logger.getLogger("main");

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
				log.error("Failed to load db driver, " + e.getMessage());
			}
	
			long startTime = System.currentTimeMillis();
			// Select Work List
			ArrayList<String> eidList = select();
			if (eidList == null) {
				log.error("No Update target File");
				return;
			} else {
				updateStatus(eidList);
			}
			count = eidList.size();
			
			long endTime = System.currentTimeMillis();
			pTime = (endTime-startTime)/1000;
		}  catch (Exception ex) {
					log.error("Proc Call error, " + ex.getMessage());
				}finally {
					Calendar calendar = Calendar.getInstance();
					int year = calendar.get(Calendar.YEAR);
					int month = calendar.get(Calendar.MONTH) + 1; // 월은 0부터 시작하므로 1을 더해줌
					int day = calendar.get(Calendar.DAY_OF_MONTH);
					int hour = calendar.get(Calendar.HOUR_OF_DAY);
					int minute = calendar.get(Calendar.MINUTE);
					int second = calendar.get(Calendar.SECOND);

					log.debug("=============================================");
					log.debug("START TIME\t\t: " + year + "-" + month + "-" + day + " " + hour + ":" + minute + ":" + second);
					log.debug("TOTAL COUNT\t\t: " + count);
					log.debug("SUCCESS COUNT\t\t: " + succCount);
					log.debug("FAIL COUNT(DOWN)\t: " + failCount);
					log.debug("TOTAL TIME \t\t: " + pTime + " sec");
					log.debug("=============================================");				
				}
	}

	public Connection getConnection(String url, String id, String pwd) {
		try {
			conn = DriverManager.getConnection(url, id, pwd);
			conn.setAutoCommit(false);
			
		} catch (SQLException e) {
			log.error("DB Connection error : " + e.getMessage());
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
					log.error("[ERROR]there's no ELEMENTID");
					
					continue;
				}
				ecmList.add(rs.getString("ELEMENTID"));
			}
		} catch (SQLException e) {
			log.error("Select Update List Error, " + e.getMessage());			
		} finally {
			try { 
				if (rs != null) { rs.close(); }
				if (pstmt != null) { pstmt.close(); }
//				if (conn != null) { conn.close(); }
			} catch (SQLException e) {
				log.error("open cursor close error, " + e.getMessage());
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
					log.error("[DBUpdate]ExcuteUpdate is Failed, " + eid);
					failCount += 1;
				}
			}
			conn.commit();
		} catch (SQLException e) {
			try {				
				if (conn != null) { conn.rollback(); }							
			} catch (SQLException e1) {
				log.error("Failed to rollback, " + e1.getMessage());
			}
			log.error("update status Error, " + e.getMessage());
		} finally {
			try {
				if (pstmt != null) { pstmt.close(); } 
//				if (conn != null) { conn.close(); }
			} catch (SQLException e) {
				log.error("open cursor close error, " + e.getMessage());
			}
		}
	}
	
	public void disconnDB() {
		try {
			if (conn != null) { conn.close(); }			
		} catch (Exception e) {
			log.error("Db DisConnect is failure");
		}
	}
}
