package edu.uchicago.mpcs53013;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

public class DemoSerialization {

	public static void main(String[] args) {
		ThriftWriter thriftOut = new ThriftWriter(new File("/tmp/thrift32.out"));
		try {
			// Open writer
			thriftOut.open();
			
			InputStream is = new FileInputStream("/tmp/710730-99999-1990.op");
			BufferedReader bReader = new BufferedReader(new InputStreamReader(is));
			
//			  0 public long STN; // required
//			  1 public long WBAN; // required
//			  2 public long YEARMODA; // required
//			  3 public double TMP; // required
//			  4 public long TMPCount; // required
//			  5 public double DEWP; // required
//			  6 public long DEWPCount; // required
//			  7 public double SLP; // required
//			  8 public long SLPCount; // required
//			  9 public double STP; // required
//			  10 public long STPCount; // required
//			  11 public double VISIB; // required
//			  12 public long VISIBCount; // required
//			  13 public double WDSP; // required
//			  14 public long WDSPCount; // required
//			  15 public double MAXSPD; // required
//			  16 public double GUST; // required
//			  17 public double MAX; // required
//			  public java.lang.String MAXFlag; // required
//			  18 public double MIN; // required
//			  public java.lang.String MINFlag; // required
//			  19 public double PRCP; // required
//			  public java.lang.String PRCPFlag; // required
//			  20 public double SNDP; // required
//			  21 public java.lang.String FRSHTT; // required
			
			String line = null;
			bReader.readLine(); // skip first line
			while( (line = bReader.readLine()) != null)
			{
				System.out.println(line);
				String[] data = line.split("\\s+");
				
				// flags for MAX, MIN, and PRCP
				String MAXflag = " ";
				double MAX = 0.0;
				String MINflag = " ";
				double MIN = 0.0;
				String PRCPflag = data[19].substring(data[19].length() - 1);
				double PRCP = Double.parseDouble(data[19].substring(0, data[19].length() - 1));
				if (data[17].substring(data[17].length() - 1).equals("*"))
				{
					MAXflag = "*";
					MAX = Double.parseDouble(data[17].substring(0, data[17].length() - 1));
				}
				else
				{
					MAX = Double.parseDouble(data[17]);
				}
				if (data[18].substring(data[18].length() - 1).equals("*"))
				{
					MINflag = "*";
					MIN = Double.parseDouble(data[18].substring(0, data[18].length() - 1));
				}
				else
				{
					MIN = Double.parseDouble(data[18]);
				}
				
				// set all variables	
				WeatherData wdata = new WeatherData();
				wdata.setSTN(Integer.parseInt(data[0]));
				wdata.setWBAN(Integer.parseInt(data[1]));
				wdata.setYEARMODA(Integer.parseInt(data[2]));
				wdata.setTMP(Double.parseDouble(data[3]));
				wdata.setTMPCount(Integer.parseInt(data[4]));
				wdata.setDEWP(Double.parseDouble(data[5]));
				wdata.setDEWPCount(Integer.parseInt(data[6]));
				wdata.setSLP(Double.parseDouble(data[7]));
				wdata.setSLPCount(Integer.parseInt(data[8]));
				wdata.setSTP(Double.parseDouble(data[9]));
				wdata.setSTPCount(Integer.parseInt(data[10]));
				wdata.setVISIB(Double.parseDouble(data[11]));
				wdata.setVISIBCount(Integer.parseInt(data[12]));
				wdata.setWDSP(Double.parseDouble(data[13]));
				wdata.setWDSPCount(Integer.parseInt(data[14]));
				wdata.setMAXSPD(Double.parseDouble(data[15]));
				wdata.setGUST(Double.parseDouble(data[16]));
				wdata.setMAX(MAX);
				wdata.setMAXFlag(MAXflag);
				wdata.setMIN(MIN);
				wdata.setMINFlag(MINflag);
				wdata.setPRCP(PRCP);
				wdata.setPRCPFlag(PRCPflag);
				wdata.setSNDP(Double.parseDouble(data[20]));
				wdata.setFRSHTT(data[21]);
				
				thriftOut.write(wdata);
			}
			
			is.close();

			// Close the writer
			thriftOut.close();
		} catch (Exception e) {
			
		}
	}

}
