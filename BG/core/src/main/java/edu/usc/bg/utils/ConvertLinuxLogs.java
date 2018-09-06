package edu.usc.bg.utils;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;



public class ConvertLinuxLogs {

	/**
	 * @param args
	 */
	static boolean firstLine = true;
	static String [] date = null;
	static boolean flag = true;
	static int increment = 0;
	public static final boolean printLinuxHeaders = false;
	public static final String memoryFileStr = "mem";
	public static final String cpuFileStr = "cpu";
	public static final String networkFileStr = "net";
	public static final String diskFileStr = "disk";
	public static final String RESULT_FILE_NAME = "_linuxutil.csv";
	public static final long NETWORK_MAX_BANDWIDTH = 1000000000L;
	
	public static String NETWORK_INTERFACE_NAME = "bond0";
	public static final String DISK_INTERFACE_NAME = "dev8-0";

	static BufferedReader memReader = null;
	static BufferedReader cpuReader = null;
	static BufferedReader netReader = null;
	static BufferedReader diskReader = null;
	
	static BufferedReader memoryReader = null;
	static BufferedReader networkReader = null;
	
	static FileWriter fw = null;
	//static BufferedWriter out = null;
	public static void main(String[] args) {
		String pythonscriptpath="/home/yaz/BG/python-plots/scripts/camp/";
//		ConvertFilesToCSV("/home/hieun/Dropbox/ExpData/#5/Rerun600s/Fix2ExpsNoCacheWithWarmup/", "dblab220", "eth5");
		//ConvertFilesToCSV("/home/yaz/mar/Experiments/anti/RatingExp1448312894095-nummembers10000-numclients1-bmodepartitioned-threadcount10-logfalse-workloadViewProfileAction-clientvoltdb.VoltdbClientFile1-vp-anticache.txt/", "hstore");
	CreateResourcesFiles.createThruCharts("/home/yaz/mar/Experiments/anti/RatingExp1448346371874-nummembers100000-numclients5-bmodepartitioned-threadcount16-logfalse-workloadViewProfileAction-clientvoltdb.VoltdbClientFile1-vp-anticache.txt/", pythonscriptpath);
	
	}
	
	public static void ConvertFilesToCSV(String directory, String file_prefix) {
		String CPUFile = directory + file_prefix + cpuFileStr + ".txt";
		String DiskFile = directory + file_prefix + diskFileStr + ".txt";
		String MemFile = directory + file_prefix + memoryFileStr + ".txt";
		String NetFile = directory + file_prefix + networkFileStr + ".txt";
		ConvertCPUFilesToCSV(CPUFile, file_prefix);
		ConvertMemFilesToCSV(MemFile, file_prefix);
		ConvertDiskFilesToCSV(DiskFile, file_prefix);
		ConvertNetFilesToCSV(NetFile, file_prefix);
	}
	
	
	
	

	private static void ConvertCPUFilesToCSV(String file, String file_prefix) {
		File cpufile = null;
		BufferedWriter outcpu = null;
		boolean gotHeaders = false;

		try {
			cpufile = new File(file);
			cpuReader = new BufferedReader(new FileReader(cpufile));
			String outFile = cpufile.getParent() + "/" + file_prefix + cpuFileStr + ".txt";
			outcpu = new BufferedWriter(new FileWriter(outFile));

			String cpuText = null;

			while ((cpuText = cpuReader.readLine()) != null) {
				while (!cpuText.contains("%idle")) {
					cpuText = cpuReader.readLine();
					if (cpuText == null)
						break;
				}

				if (cpuText != null) {
					String[] cpuLine = null;
					String cpuOutLine = "";
					String cpuHeadersLine = "";
					while (!cpuText.equals("")) {
						cpuText = cpuReader.readLine();
						if (cpuText == null || cpuText.length() == 0)
							break;
						cpuLine = cpuText.split("\\s+");
						double i = Double.parseDouble(cpuLine[8]);
						i = 100 - i;
						if(!gotHeaders)
							cpuHeadersLine += cpuLine[2] + ",";
						cpuOutLine = cpuOutLine + i + ",";
					}
					if(!gotHeaders){
						outcpu.write(cpuHeadersLine + System.getProperty("line.separator"));
						gotHeaders = true;
					}
					outcpu.write(cpuOutLine + System.getProperty("line.separator"));
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace(System.out);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		} catch (NullPointerException e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (cpuReader != null) {
					cpuReader.close();
				}
				outcpu.close();

			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}

	}

	private static void ConvertMemFilesToCSV(String file, String file_prefix) {
		File memfile = null;
		BufferedWriter outmem = null;

		try {
			memfile = new File(file);
			memReader = new BufferedReader(new FileReader(memfile));
			String outFile = memfile.getParent() + "/" + file_prefix + memoryFileStr + ".txt";
			outmem = new BufferedWriter(new FileWriter(outFile));

			String memText = null;
			memReader.readLine();
			memReader.readLine();
			memReader.readLine();

			while ((memText = memReader.readLine()) != null) {
				String[] memLine = memText.split("\\s+");
				double i = (Double.parseDouble(memLine[2]) + Double.parseDouble(memLine[6])) / 1024;
				outmem.write(i + "," + System.getProperty("line.separator"));
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace(System.out);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		} catch (NullPointerException e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (memReader != null) {
					memReader.close();
				}
				outmem.close();

			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}

	}

	private static void ConvertNetFilesToCSV(String file, String file_prefix) {
		File netfile = null;
		BufferedWriter outnet = null;
		boolean gotHeaders = false;

		try {
			netfile = new File(file);
			netReader = new BufferedReader(new FileReader(netfile));
			String outFile = netfile.getParent() + "/" + file_prefix + networkFileStr + ".txt";
			outnet = new BufferedWriter(new FileWriter(outFile));

			String netText = null;

			while ((netText = netReader.readLine()) != null) {
				while (!netText.contains("IFACE")) {
					netText = netReader.readLine();
					if (netText == null)
						break;
				}

				if (netText != null) {
					String[] netLine = null;
					String netOutLine = "";
					String netHeadersLine = "";
					while (!netText.equals("")) {
						netText = netReader.readLine();
						if (netText == null || netText.length() == 0)
							break;
						netLine = netText.split("\\s+");

						double rx = Double.parseDouble(netLine[5]);
						double tx = Double.parseDouble(netLine[6]);
						double i = (rx + tx) / 1024;
						if(!gotHeaders)
							netHeadersLine += netLine[2] + "," + netLine[2] + "_R," + netLine[2] + "_T,";
						netOutLine += i + "," + (rx/1024) + "," + (tx/1024) + ",";
					}
					if(!gotHeaders){
						outnet.write(netHeadersLine + System.getProperty("line.separator"));
						gotHeaders = true;
					}
					outnet.write(netOutLine + System.getProperty("line.separator"));
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace(System.out);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		} catch (NullPointerException e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (netReader != null) {
					netReader.close();
				}
				outnet.close();

			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}

	}

	private static void ConvertDiskFilesToCSV(String file, String file_prefix) {
		File diskfile = null;
		BufferedWriter outdisk = null;
		BufferedWriter outdiskRW = null;
		boolean gotHeaders = false;

		try {
			diskfile = new File(file);
			diskReader = new BufferedReader(new FileReader(diskfile));
			String outFile = diskfile.getParent() + "/" + file_prefix + diskFileStr + ".txt";
			String outRWFile = diskfile.getParent() + "/" + file_prefix + diskFileStr + "RW.txt";
			outdisk = new BufferedWriter(new FileWriter(outFile));
			outdiskRW = new BufferedWriter(new FileWriter(outRWFile));

			String diskText = null;

			while ((diskText = diskReader.readLine()) != null) {
				while (!diskText.contains(DISK_INTERFACE_NAME)) {
					diskText = diskReader.readLine();
					if (diskText == null)
						break;
				}

				if (diskText != null) {
					String[] diskLine = null;
					diskLine = diskText.split("\\s+");
					if(!gotHeaders){
						outdisk.write(diskLine[2] + "," + System.getProperty("line.separator"));
						outdiskRW.write(diskLine[2] + "_R," + diskLine[2] + "_W," + System.getProperty("line.separator"));
						gotHeaders = true;
					}
					outdisk.write(diskLine[7] + "," + System.getProperty("line.separator"));
					outdiskRW.write(diskLine[5] + "," + diskLine[6] + "," + System.getProperty("line.separator"));
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace(System.out);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		} catch (NullPointerException e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (diskReader != null) {
					diskReader.close();
				}
				outdisk.close();
				outdiskRW.close();

			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}

	}



	
	/***
     * Find the full file names for any files in the directory that match the
     * logfile prefixes specified in logfiles.
     * @param directory
     * @param logfiles
     * @return
     */
    public static String[] findActualLogNames(String directory, String[]logfiles)
    {
    	File dir =  new File(directory);
//    	FilenameFilter filter = new FilenameFilter() {
//    		private String prefix;
//    		public boolean accept (File dir, String name) {
//    			return name.startsWith(prefix);
//    		}
//    		
//    		public void setPrefix(String name) {
//    			prefix = name;
//    		}    		
//    	};
    	
    	String files[] = dir.list();;
    	String results[] = new String[logfiles.length];
    	for(int i = 0; i < logfiles.length; i++) {
    		for(int j = 0; j < files.length; j++) {
    			if(files[j].contains(logfiles[i])) {
    				results[i] = files[j];
    				break;
    			}
    		}
    	}
    	
    	for(int i = 0; i < results.length; i++)
    	{
    		if(results[i] == null)
    		{
    			System.out.println("ERROR. No matching file for a specified log file prefix: "+logfiles[i]);
    			return null;
    		}
    	}
    	return results;    	    	
    }

	public static void ConvertFilesToCSV (String directory, String file_prefix, String network_interface_name, int numCPUs,boolean reportAllCPUs) {
		String [] files = {
				file_prefix + memoryFileStr, 
				file_prefix + cpuFileStr, 
				file_prefix + networkFileStr, 
				file_prefix + diskFileStr};
		ConvertFilesToCSV(directory, findActualLogNames(directory,files), 
				file_prefix, network_interface_name,numCPUs,reportAllCPUs);
	}
	
	public static void ConvertFilesToCSV (String directory, String [] files, String file_prefix, String network_interface_name, int numCPUs, boolean reportAllCPUs){

		File memfile=null;
		File cpufile=null;
		File netfile=null;
		File diskfile=null;
		BufferedWriter outmem = null;
		BufferedWriter outdisk = null;
		BufferedWriter outnet = null;
		BufferedWriter outcpu = null;

		try {



			if( files.length != 4 )
			{
				System.out.println("ConvertFilesToCSV: Error invalid number of input files");
			}

			NETWORK_INTERFACE_NAME = network_interface_name;
			memfile=new File(directory + files[0]);
			cpufile=new File(directory + files[1]);
			netfile=new File(directory + files[2]);
			diskfile=new File(directory + files[3]);

			memoryReader = new BufferedReader(new FileReader(memfile));
			cpuReader = new BufferedReader(new FileReader(cpufile));
			networkReader = new BufferedReader(new FileReader(netfile));
			diskReader = new BufferedReader(new FileReader(diskfile));


			outmem = new BufferedWriter(new FileWriter(directory + file_prefix + memoryFileStr+".txt"));
			outdisk = new BufferedWriter(new FileWriter(directory + file_prefix + diskFileStr+".txt"));
			outcpu = new BufferedWriter(new FileWriter(directory + file_prefix + cpuFileStr+".txt"));
			outnet = new BufferedWriter(new FileWriter(directory + file_prefix + networkFileStr+".txt"));

			String memoryText = null;
			String cpuText = null;
			String networkText = null;
			String diskText = null;

			firstLine = true;
			while (true) {
				String result = "";
				//start reading all 3 files line by line

				memoryText = memoryReader.readLine();
				cpuText = cpuReader.readLine();
				networkText = networkReader.readLine();
				diskText = diskReader.readLine();

				//if end of line then break
				if(memoryText == null || cpuText == null || networkText == null || diskText == null)
					break;

				if(firstLine){// this section is for the headers of each file
					firstLine = false;
					if(printLinuxHeaders) {
						//	out.write(memoryText+"\n");//print the file header
					}
					date = memoryText.split("\t");//extract the date when the file was generated from the first line

					// jump 2 line
					memoryText = memoryReader.readLine();
					cpuText = cpuReader.readLine();
					networkText = networkReader.readLine();
					diskText = diskReader.readLine();

					memoryText = memoryReader.readLine();
					cpuText = cpuReader.readLine();
					networkText = networkReader.readLine();
					diskText = diskReader.readLine();

					//extract column names from each file and put each in a separate array 
					String[] memLine = memoryText.split("\\s+"); 
					String[] cpuLine = cpuText.split("\\s+"); 
					String[] netLine = networkText.split("\\s+"); 
					//					String[] diskLine = diskText.split("\\s+");

					//write down all the information to the file
					result = date[1].concat(memLine[0]+" "+memLine[1]+","+memLine[4]+",");
					result = result.concat(cpuLine[8]+",");
					result = result.concat(netLine[5]+","+netLine[6]);

					if(printLinuxHeaders) {
						//out.write(result+"\n");
					}
					//out.write("date, available kbytes, % Processor Time, " +
					//	"rxkB/s, txkB/s, Bytes Total/sec, " +
					//"Estimated Current Bandwidth, " +
					//"Avg. Disk Queue Length\n");

				}
				else{// this section is for reading all the numbers

					while(!networkText.contains(NETWORK_INTERFACE_NAME)){//get the line which has information about the network card
						networkText = networkReader.readLine();
					}
					while(cpuText.contains("%idle") || ! cpuText.matches(".*\\d+.*")){//get the line which has information about CPU
						cpuText = cpuReader.readLine();
					}
					while(!diskText.contains(DISK_INTERFACE_NAME)) {
						diskText = diskReader.readLine();
					}

					//extract all the information from each file and put each in a separate array
					String[] memLine = memoryText.split("\\s+"); 
					String[] cpuLine = cpuText.split("\\s+"); 
					String[] netLine = networkText.split("\\s+"); 
					String[] diskLine = diskText.split("\\s+");


					//this part is responsible for incrementing the date when switching from PM to AM
					if(memLine[1].equalsIgnoreCase("AM") && flag == false){
						flag = true;
						increment++;
					}
					else if(memLine[1].equalsIgnoreCase("PM"))
						flag = false;

					SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
					Calendar c = Calendar.getInstance();
					c.setTime(sdf.parse(date[1]));
					c.add(Calendar.DATE, increment);
					String newDate = sdf.format(c.getTime()); 

					// Convert date to 24 hour format
					String datetime = newDate.concat(" "+memLine[0]+" "+memLine[1]+","+memLine[4]);
					SimpleDateFormat fmt12 = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss aa");
					SimpleDateFormat fmt24 = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS");

					try {
						result = fmt24.format(fmt12.parse(datetime)) + ", ";
					} catch( ParseException e ) {
						e.printStackTrace();
					}


					//write down all the information to the file
					//result = newDate.concat(" "+memLine[0]+" "+memLine[1]+","+memLine[4]+",");
					double i = Double.parseDouble(memLine[2])/1024; 
					outmem.write(i+","+System.getProperty("line.separator"));
					i = Double.parseDouble(cpuLine[8]);
					i = 100 - i;;
					String cpuOutLine=i+",";
					for (int j=0;j<numCPUs;j++){

						cpuText = cpuReader.readLine();
						if (reportAllCPUs){
							cpuLine = cpuText.split("\\s+"); 
							i = Double.parseDouble(cpuLine[8]);
							i = 100 - i;
							cpuOutLine=cpuOutLine+i+",";
						}
					}
					outcpu.write(cpuOutLine+System.getProperty("line.separator"));
					DecimalFormat df = new DecimalFormat("#.##");
					result = result.concat(df.format(i)+", ");
					result = result.concat(netLine[5]+", "+netLine[6]);

					double rx = Double.parseDouble(netLine[5]);
					double tx = Double.parseDouble(netLine[6]);
					result += ", " + ((rx + tx)*1024);
					i=(rx + tx)/1024;
					outnet.write(i+","+System.getProperty("line.separator"));
					result += ", " + NETWORK_MAX_BANDWIDTH;
					result += ", " + diskLine[7];
					outdisk.write(diskLine[7]+","+System.getProperty("line.separator"));
					result += ", " + ((rx + tx) * 1024) / NETWORK_MAX_BANDWIDTH;

					//out.write(result+"\n");

				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} finally {
			try {

				if (memoryReader != null) {
					memoryReader.close();
				}
				if (cpuReader != null) {
					cpuReader.close();
				}
				if (networkReader != null) {
					networkReader.close();
				}

				outdisk.close();
				outmem.close();
				outnet.close();
				outcpu.close();
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}

	}
}
