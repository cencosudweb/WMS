package corp.cencosud.roble;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class InicioPrograma {

	private static BufferedWriter bw;
	private static String path;

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		Map <String, String> mapArguments = new HashMap<String, String>();
		String sKeyAux = null;

		for (int i = 0; i < args.length; i++) {

			if (i % 2 == 0) {

				sKeyAux = args[i];
			}
			else {

				mapArguments.put(sKeyAux, args[i]);
			}
		}

		try {

			File info              = null;
			File miDir             = new File(".");
			path                   =  miDir.getCanonicalPath();
			info                   = new File(path+"/info.txt");
			bw = new BufferedWriter(new FileWriter(info));
			info("El programa se esta ejecutando...");
			crearTxt(mapArguments);
			System.out.println("El programa finalizo.");
			info("El programa finalizo.");
			bw.close();

		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private static void crearTxt(Map <String, String> mapArguments) {

		Connection dbconnOracle    = crearConexionOracle();
		File file1              = null;
		BufferedWriter bw       = null;
		PreparedStatement pstmt = null;
		StringBuffer sb         = null;
		
		String iFechaIni        = null;
		String iFechaFin        = null;

		try {

			try {

				iFechaFin = restarDia(mapArguments.get("-fi"));
				iFechaIni = restarSieteDia(mapArguments.get("-fi"));
				
				System.out.println("iFechaIni: "+iFechaIni);
				System.out.println("iFechaFin: "+iFechaFin);

			}
			catch (Exception e) {

				e.printStackTrace();
			}
			
			file1                   = new File(path + "/WMS-" + iFechaFin + ".txt");
			
			sb = new StringBuffer();
			sb.append("SELECT ");
			sb.append("PT.MARK_FOR || '-' || IM.SIZE_DESC AS LLAVE, ");
			sb.append("PT.MARK_FOR AS NRO_ORDEN, ");
			sb.append("DT.PKT_CTRL_NBR AS PICK_TICKET, ");
			sb.append("CH.WAVE_NBR AS NRO_OLA, ");
			sb.append("TO_CHAR(WP.CREATE_DATE_TIME, 'DD-MM-YYYY HH24:MI:SS') AS FECHA_INICIO_OLA, ");
			sb.append("TO_CHAR(WP.WAVE_STAT_DATE_TIME, 'DD-MM-YYYY HH24:MI:SS') AS FECHA_TERMINO_OLA, ");
			sb.append("LD.LOAD_NBR AS NRO_CARGA, ");
			sb.append("TO_CHAR(LD.LAST_INVC_DATE_TIME, 'DD-MM-YYYY HH24:MI:SS') AS FECHA_CIERRE_CARGA, ");
			sb.append("IM.SIZE_DESC AS SKU, ");
			sb.append("TO_CHAR(PT.CREATE_DATE_TIME, 'DD-MM-YYYY HH24:MI:SS') AS FEC_CREA_HD_PKT, ");
			sb.append("TO_CHAR(PT.MOD_DATE_TIME, 'DD-MM-YYYY HH24:MI:SS') AS FEC_MOD_HD_PKT, ");
			sb.append("DT.PKT_SEQ_NBR AS PKT_SEQ_NBR, ");
			sb.append("DT.ORIG_PKT_QTY AS CANT_PKT ");
			
			sb.append("FROM PKT_HDR PT ");
			sb.append("INNER JOIN PKT_DTL DT ");
			sb.append("ON PT.PKT_CTRL_NBR = DT.PKT_CTRL_NBR ");
			sb.append("INNER JOIN ITEM_MASTER IM ");
			sb.append("ON DT.SKU_ID = IM.SKU_ID ");
			sb.append("INNER JOIN CARTON_HDR CH ");
			sb.append("ON PT.PKT_CTRL_NBR = CH.PKT_CTRL_NBR AND DT.SKU_ID = CH.SKU_ID ");
			sb.append("INNER JOIN WAVE_PARM WP ");
			sb.append("ON WP.WAVE_NBR = CH.WAVE_NBR ");
			sb.append("INNER JOIN OUTBD_LOAD LD ");
			sb.append("ON CH.LOAD_NBR = LD.LOAD_NBR ");
			
			sb.append("WHERE ");
			sb.append("PT.MARK_FOR IS NOT NULL AND ");
			sb.append("WP.CREATE_DATE_TIME BETWEEN ");
			
			sb.append("TO_DATE('");
			sb.append(iFechaIni);
			sb.append(" 00:00:00");
			sb.append("', 'DD-MM-YYYY HH24:MI:SS') AND ");
			
			sb.append("TO_DATE('");
			sb.append(iFechaFin);
			sb.append(" 23:59:59");
			sb.append("', 'DD-MM-YYYY HH24:MI:SS') ");
			
			sb.append("GROUP BY ");
			sb.append("PT.MARK_FOR || '-' || IM.SIZE_DESC, ");
			sb.append("PT.MARK_FOR, ");
			sb.append("DT.PKT_CTRL_NBR, ");
			sb.append("CH.WAVE_NBR, ");
			sb.append("TO_CHAR(WP.CREATE_DATE_TIME, 'DD-MM-YYYY HH24:MI:SS'), ");
			sb.append("TO_CHAR(WP.WAVE_STAT_DATE_TIME, 'DD-MM-YYYY HH24:MI:SS'), ");
			sb.append("LD.LOAD_NBR, ");
			sb.append("TO_CHAR(LD.LAST_INVC_DATE_TIME, 'DD-MM-YYYY HH24:MI:SS'), ");
			sb.append("IM.SIZE_DESC, ");
			sb.append("PT.CREATE_DATE_TIME, PT.MOD_DATE_TIME, ");
			sb.append("DT.CREATE_DATE_TIME, DT.MOD_DATE_TIME, ");
			sb.append("DT.PKT_SEQ_NBR, ");
			sb.append("DT.ORIG_PKT_QTY ");

			pstmt        = dbconnOracle.prepareStatement(sb.toString());
			sb           = new StringBuffer();
			ResultSet rs = pstmt.executeQuery();
			bw           = new BufferedWriter(new FileWriter(file1));
 
			bw.write("LLAVE;");
			bw.write("NRO_ORDEN;");
			bw.write("PICK_TICKET;");
			bw.write("NRO_OLA;");
			bw.write("FECHA_INICIO_OLA;");
			bw.write("FECHA_TERMINO_OLA;");
			bw.write("NRO_CARGA;");
			bw.write("FECHA_CIERRE_CARGA;");
			bw.write("SKU;");
			bw.write("FEC_CREA_HD_PKT;");
			bw.write("FEC_MOD_HD_PKT;");
			bw.write("PKT_SEQ_NBR;");
			bw.write("CANT_PKT;\n");

			while (rs.next()) {
				
				bw.write(rs.getString("LLAVE") + ";");
				bw.write(rs.getString("NRO_ORDEN") + ";");
				bw.write(rs.getString("PICK_TICKET") + ";");
				bw.write(rs.getString("NRO_OLA") + ";");
				bw.write(rs.getString("FECHA_INICIO_OLA") + ";");
				bw.write(rs.getString("FECHA_TERMINO_OLA") + ";");
				bw.write(rs.getString("NRO_CARGA") + ";");
				bw.write(rs.getString("FECHA_CIERRE_CARGA") + ";");
				bw.write(rs.getString("SKU") + ";");
				bw.write(rs.getString("FEC_CREA_HD_PKT") + ";");
				bw.write(rs.getString("FEC_MOD_HD_PKT") + ";");
				bw.write(rs.getString("PKT_SEQ_NBR") + ";");
				bw.write(rs.getString("CANT_PKT") + "; \n");
			
			}

			info("Archivos creados.");
		}
		catch (Exception e) {

			info("[crearTxt1]Exception:"+e.getMessage());
			
		}
		finally {

			cerrarTodo(dbconnOracle,pstmt,bw);
			
		}
	}

	private static Connection crearConexionOracle() {
		System.out.println("Creando conexion a WMS.");

		Connection dbconnection = null;

		try {

			Class.forName("oracle.jdbc.driver.OracleDriver");
			
			//El servidor g500603sv0zt corresponde a Producción.
			dbconnection = DriverManager.getConnection("jdbc:oracle:thin:@g500603svbbr:1521:REPORTMHN","CONWMS","CONWMS");
			System.out.println("Conexion a WMS CREADA.");
		}
		catch (Exception e) {

			e.printStackTrace();
		}
		return dbconnection;
	}

	private static void cerrarTodo(Connection cnn, PreparedStatement pstmt, BufferedWriter bw){

		try {

			if (cnn != null) {

				cnn.close();
				cnn = null;
			}
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (pstmt != null) {

				pstmt.close();
				pstmt = null;
			}
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (bw != null) {

				bw.flush();
				bw.close();
				bw = null;
			}
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
	}

	private static void info(String texto){

		try {

			bw.write(texto+"\n");
			bw.flush();
		}
		catch (Exception e) {

			System.out.println("Exception:"+e.getMessage());
		}
	}

	private static String restarDia(String sDia) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");

		String dia = "";
		String sFormato = "yyyyMMdd";
		Calendar diaAux = null;
		//String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormato);
			diaAux.setTime(df.parse(sDia));
			//diaAux.add(Calendar.DAY_OF_MONTH, -1);
			//sDiaAux = df.format(diaAux.getTime());
			//dia = Integer.parseInt(sDiaAux);
			dia = sdf.format(diaAux.getTime());
			
		}
		catch (Exception e) {

			info("[restarDia]Exception:"+e.getMessage());
		}

		return dia;
	}
	
	private static String restarSieteDia(String sDia) {

		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
		
		String dia = "";
		String sFormato = "yyyyMMdd";
		Calendar diaAux = null;
		//String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormato);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -7);
			//sDiaAux = df.format(diaAux.getTime());
			//dia = Integer.parseInt(sDiaAux);
			
			dia = sdf.format(diaAux.getTime());
			
		}
		catch (Exception e) {

			info("[restarDia]Exception:"+e.getMessage());
		}
		return dia;
	}
}
