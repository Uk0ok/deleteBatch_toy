import java.io.FileReader;
import java.util.Properties;

import org.apache.log4j.Logger;

public class DeleteMain {

	private static Logger log = Logger.getLogger("main");

	public static String DB_DRIVER = null;
	public static String DB_URL = null;
	public static String DB_ID = null;
	public static String DB_PWD = null;
	
	public static String ECM_IP = null; 
	public static int ECM_PORT = 0;
	public static String ECM_ID = null; 
	public static String ECM_PWD = null;
	public static String ECM_GW = null;

	public static int rownum = 0;
	public static String DATEFORMAT = null;

	public static String SELECTQUERY = null;
	public static String UPDATEQUERY = null;

	public static void main(String[] args) {
		FileReader fr;

		log.debug(" -- Process Start -- ");

		try {
			fr = new FileReader("./conf/conf.properties");
			Properties properties = new Properties();
			properties.load(fr);
			
			ECM_IP  = properties.getProperty("ECM.IP");
			ECM_PORT= Integer.parseInt(properties.getProperty("ECM.PORT"));
			ECM_ID  = properties.getProperty("ECM.ID");
			ECM_PWD = properties.getProperty("ECM.PWD");
			ECM_GW  = properties.getProperty("ECM.GW");
			
			DB_DRIVER = properties.getProperty("DB.DRIVER");
			DB_URL    = properties.getProperty("DB.URL");
			DB_ID 	  = properties.getProperty("DB.ID");
			DB_PWD 	  = properties.getProperty("DB.PWD");
			rownum = Integer.parseInt(properties.getProperty("DB.ROWNUM"));

			SELECTQUERY = properties.getProperty("SQL.SELECTQUERY");
			UPDATEQUERY = properties.getProperty("SQL.UPDATEQUERY");
		} catch (Exception e) {
			log.error(" Properties Load Error, "+ e.getMessage());
			return;
		}
		
		DeleteRun delRun = new DeleteRun();
		delRun.run();
		delRun.disconnDB();

		log.debug(" -- Process End -- ");
	}
}
