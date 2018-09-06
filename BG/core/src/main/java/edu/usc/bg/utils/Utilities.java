package edu.usc.bg.utils;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import edu.usc.bg.base.BGConstants;



public class Utilities {

	public static final String M_RESTORE_CHECK_CMD = "mresc";

	public static final String M_RESTORE_CMD = "mrest";

	public static final String M_DELETE_CMD = "mdele";

	public static final String M_DELETE_CHECK_CMD = "mdelc";

	public static int numAttepmts = 10;

	static String os = "windows";

	static String folderNotAvailableMsg = "DIR DOES NOT EXIST";

	static String copyingFailedMsg = "COPYING FAILED";

	static final String NORMAL_CMD = "norml";

	static final String FOLDER_AVAILABILITY_CMD = "check";

	static final String FOLDER_COPY_CMD = "xcopy";

	static final String MYSQL_RUN_CMD = "mysql";

	static final String ORACLE_RUN_CMD = "orcle";

	static final String RESTORE_DB_CMD = "restr";

	static final String RESTORE2_DB_CMD = "rstrw";

	static String dbipOracle = "10.0.0.65";

	static String dbipMySQL = "10.0.0.245";

	static int LPort = 11111;

	public static boolean runningFromLinux=true;

	public static String sshNoCeck=" -o StrictHostKeyChecking=no ";

	public static String cygwinRemoteSSHCmdFormat="C:/cygwin64/bin/ssh " +sshNoCeck+ "  %s \" %s \"";

	public static String linuxRemoteSSHCmdFormat="ssh "+sshNoCeck+" %s  %s ";

	public static boolean antiCacheInit=false;
	/////////////////////
	

		public enum DBState {
			Running, Stopped, Other;
		}

		private static void stopLinuxMySQL(String IP, String User) {
			PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

			String cmd = " sudo service mysql stop ";
			printWriter.println(cmd + "\n");
			System.out.println("Running command on [" + IP + "]: " + cmd);
			// System.out.println("Adding: " + cmd);

			printWriter.close();

			executeRuntime("chmod +x " + BGConstants.CommandFile, true);

			printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
			cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
			printWriter.println(cmd + "\n");
			// System.out.println("Writing to BGConstants.SSHCommandFile: " + cmd);

			printWriter.close();

			executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
			executeRuntime(BGConstants.SSHCommandFile, false);

			if (LinuxMySQLState(IP, User) != DBState.Stopped)
				stopLinuxMySQL(IP, User);

		}

		private static void startLinuxMySQL(String IP, String User) {
			PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

			String cmd = " sudo service mysql start ";
			printWriter.println(cmd + "\n");
			System.out.println("Running command on [" + IP + "]: " + cmd);
			// System.out.println("Adding: " + cmd);

			printWriter.close();

			executeRuntime("chmod +x " + BGConstants.CommandFile, true);

			printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
			cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
			printWriter.println(cmd + "\n");
			// System.out.println("Writing to SSHCommandFile: " + cmd);

			printWriter.close();

			executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
			executeRuntime(BGConstants.SSHCommandFile, true);

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}

			if (LinuxMySQLState(IP, User) != DBState.Running)
				startLinuxMySQL(IP, User);
		}


		private static DBState LinuxMySQLState(String IP, String User) {
			PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

			String cmd = " sudo service mysql status ";
			printWriter.println(cmd + "\n");
			System.out.println("Running command on [" + IP + "]: " + cmd);
			// System.out.println("Adding: " + cmd);

			printWriter.close();

			executeRuntime("chmod +x " + BGConstants.CommandFile, true);

			printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
			cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
			printWriter.println(cmd + "\n");
			// System.out.println("Writing to SSHCommandFile: " + cmd);

			printWriter.close();

			executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
			List<String> outs = executeRuntimeReturn(BGConstants.SSHCommandFile, true);
			for (String line : outs) {
				if (line.contains("mysql start/running"))
					return DBState.Running;
				else if (line.contains("mysql stop/waiting") || line.contains("stop: Unknown instance:"))
					return DBState.Stopped;
				else if (line.contains("mysql stop/killed")) {
					try {
						Thread.sleep(30000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					KillMySQL(IP, User);
					return DBState.Stopped;
				}
			}

			return DBState.Other;

		}

		private static void KillMySQL(String IP, String User) {
			PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

			String cmd = " killall -9 mysqld ";
			printWriter.println(cmd + "\n");
			System.out.println("Running command on [" + IP + "]: " + cmd);
			// System.out.println("Adding: " + cmd);

			printWriter.close();

			executeRuntime("chmod +x " + BGConstants.CommandFile, true);

			printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
			cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
			printWriter.println(cmd + "\n");
			// System.out.println("Running command on ["+IP+"]: " + cmd);

			printWriter.close();

			executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
			executeRuntime(BGConstants.SSHCommandFile, true);

		}

		public static void restoreLinuxMySQLWithFlashCache(String IP, String User, String src, String dist) {
	        try {
	        	stopLinuxMySQL(IP, User);
	        //	DD.restoreMySQLLocations(IP);
	            System.out.println("");
	            System.out.println("===================================");
	            prepare_umount(IP, User, "/dev/mapper/cachedev");
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            System.out.println("===================================");
	            // umount /dev/mapper/cachedev
	            umount_dir(IP, User, "/dev/mapper/cachedev");
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            System.out.println("===================================");
	            // dmsetup remove cachedev
	            dmsetup(IP, User, "cachedev");
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            System.out.println("===================================");
	            // sudo flashcache_destroy /dev/sdb1
	            destroy_flashcache(IP, User, "/dev/sdb1");
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            System.out.println("===================================");
	            // deleteFolder ~/Desktop/flashcachedevice and its contents
	            deleteRemoteDir(IP, User, dist);
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            System.out.println("===================================");
	            // sudo mkfs /dev/sdb1 -t ext4
	            format(IP, User, "sdb1");
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            System.out.println("===================================");
	            // sudo mkfs /dev/sdb1 -t ext4
	            format(IP, User, "sda7");
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            System.out.println("===================================");
	            // sudo flashcache_create -p back -b 4k cachedev /dev/sdb1 /dev/sda7
	            flashcache_create(IP, User, "/dev/sdb1", "/dev/sda7");
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            System.out.println("===================================");

	            CreateRemoteDir(IP, User, dist);
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            System.out.println("===================================");

	            // sudo mount /dev/mapper/cachedev ~/Desktop/flashcachedevice/
	            mount_dir(IP, User, "/dev/mapper/cachedev", dist);
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            System.out.println("===================================");

	            // sudo chown -vR mysql:mysql ~/Desktop/flashcachedevice/
	            // sudo chmod -vR 700 ~/Desktop/flashcachedevice/
	            FlashcachePermissions(IP, User, dist);
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            System.out.println("===================================");
	            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	    		Date date = new Date();
	    		System.out.println("Start Copy at:"+dateFormat.format(date));
	            CopyFromTo2(IP, User, src, dist);
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            startLinuxMySQL(IP, User);
	        } catch (InterruptedException e) {
	            e.printStackTrace(System.out);
	        }
	    }

		public static void destroyFlashCache(String IP, String User) {
	        try {
	        	Thread.sleep(1000);
	        	stopLinuxMySQL(IP, User);
	        	
	            System.out.println("");
	            System.out.println("===================================");
	            prepare_umount(IP, User, "/dev/mapper/cachedev");
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            System.out.println("===================================");
	            // umount /dev/mapper/cachedev
	            umount_dir(IP, User, "/dev/mapper/cachedev");
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            System.out.println("===================================");
	            // dmsetup remove cachedev
	            dmsetup(IP, User, "cachedev");
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            System.out.println("===================================");
	            // sudo flashcache_destroy /dev/sdb1
	            destroy_flashcache(IP, User, "/dev/sdb1");
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            System.out.println("");
	            System.out.println("===================================");
	            
//	            // deleteFolder ~/Desktop/flashcachedevice and its contents
//	            deleteRemoteDir(IP, User, dist);
//	            System.out.println("===================================");
//	            Thread.sleep(1000);
//	            System.out.println("");
//	            System.out.println("===================================");
//	            // sudo mkfs /dev/sdb1 -t ext4
//	            format(IP, User, "sdb1");
//	            System.out.println("===================================");
//	            Thread.sleep(1000);
//	            System.out.println("");
//	            System.out.println("===================================");
//	            // sudo mkfs /dev/sdb1 -t ext4
//	            format(IP, User, "sda7");
//	            System.out.println("===================================");
//	            Thread.sleep(1000);
//	            System.out.println("");
//	            System.out.println("===================================");
//	            // sudo flashcache_create -p back -b 4k cachedev /dev/sdb1 /dev/sda7
//	            flashcache_create(IP, User, "/dev/sdb1", "/dev/sda7");
//	            System.out.println("===================================");
//	            Thread.sleep(1000);
//	            System.out.println("");
//	            System.out.println("===================================");
	//
//	            CreateRemoteDir(IP, User, dist);
//	            System.out.println("===================================");
//	            Thread.sleep(1000);
//	            System.out.println("");
//	            System.out.println("===================================");
	//
//	            // sudo mount /dev/mapper/cachedev ~/Desktop/flashcachedevice/
//	            mount_dir(IP, User, "/dev/mapper/cachedev", dist);
//	            System.out.println("===================================");
//	            Thread.sleep(1000);
//	            System.out.println("");
//	            System.out.println("===================================");
	//
//	            // sudo chown -vR mysql:mysql ~/Desktop/flashcachedevice/
//	            // sudo chmod -vR 700 ~/Desktop/flashcachedevice/
//	            FlashcachePermissions(IP, User, dist);
//	            System.out.println("===================================");
//	            Thread.sleep(1000);
//	            System.out.println("");
//	            System.out.println("===================================");
//	            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
//	    		Date date = new Date();
//	    		System.out.println("Start Copy at:"+dateFormat.format(date));
//	            CopyFromTo2(IP, User, src, dist);
//	            System.out.println("===================================");
//	            Thread.sleep(1000);
//	            System.out.println("");
//	            startLinuxMySQL(IP, User);
	        } catch (InterruptedException e) {
	            e.printStackTrace(System.out);
	        }
	    }

		
		public static void CopyFromTo2Bcache(String IP, String User, String src, String dist) {
	        PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

	        String cmd = " sudo cp -afrv " + src + " " + dist + " ";
	        printWriter.println(cmd + "\n");
	        System.out.println("[Bache]: Running command on [" + IP + "]: " + cmd);
	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.CommandFile, true);

	        printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
	        cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
	        printWriter.println(cmd + "\n");
	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
	        executeRuntime(BGConstants.SSHCommandFile, true);
	    }
		
		public static void uninstallBcache(String IP) {
			System.out.println("[Bache]: Uninstalling Bcache ");
			String cmd = "ssh "+ IP +" sudo bash ~/uninstall_bcache.sh ";
			System.out.println("[Bache]: Executing " + cmd);
			executeRuntime(cmd, true);		
		}
		
		public static void installBcache(String IP, String cachePartition, String diskPartition, String cacheBlockSize, String writePolicy) {
			System.out.println("[Bache]: Installing bcache");
			String cmd = "ssh "+ IP +" sudo bash ~/install_bcache.sh " + cachePartition + " " + diskPartition + " " + cacheBlockSize + " " + writePolicy;
			System.out.println("[Bache]: Executing " + cmd);
			executeRuntime(cmd, true);		
		}
		

		/**
		 * Checks if the respective write policy is valid for the given cache type
		 * @param hostCacheType - Cache type given
		 * @param writePolicy - write policy specified
		 * @param cacheWritePolicyMap - Map of cache types and policies
		 * @return
		 */
		public static boolean isWritePolicyValid(String hostCacheType, String writePolicy, HashMap<String, HashMap<String, String>> cacheWritePolicyMap) {
			return (cacheWritePolicyMap.get(hostCacheType).containsKey(writePolicy)) ? true : false; 
		}

		public static void buildWritePolicyMap(HashMap<String, HashMap<String, String>> cacheWritePolicyMap) {
			cacheWritePolicyMap.put("bcache", new HashMap<String, String>() {
				{
					put("writeback", "writeback");
					put("writethrough", "writethrough");
					put("writearound", "writearound");
					put("none", "none");
					put("back", "writeback");
					put("thru", "writethrough");
					put("around", "writearound");
				}
			});
			cacheWritePolicyMap.put("flash", new HashMap<String, String>() {
				{
					put("writeback", "back");
					put("writethrough", "thru");
					put("writearound", "around");
					put("back", "back");
					put("thru", "thru");
					put("around", "around");
				}
			});
			cacheWritePolicyMap.put("none", new HashMap<String, String>() );
		}

		
		
		
		/**
		 * Assumptions: 
		 * 1. There is always one bcache running in the system/ this code works only on bcache0, 
		 * 2. If there are many bcache running, change it to whatever bcache you are using like bcache0, bcache1, bcache2,etc.
		 * 
		 * @param IP - IP address of the machine where the bcache needs to be installed
		 * @param mysqlDataLoc - MySQL data location
		 * @param backupfolder - backup data folder for MySQL from where the data is copied to "mysqlDataLoc"
		 */
		public static void restoreMySQLLinuxWithBcache(String IP, HashMap<String, String> cacheSettings) {
			try {
				String bcacheDevice = "/dev/bcache0";
				String user = cacheSettings.get("dbuser").toString();
				String backupFolder = cacheSettings.get("mysqlbackupdir").toString();
				String mysqlDataLoc = cacheSettings.get("mysqldatadir").toString();
				
				System.out.println("[Bache]: Restart MySQL");
				stopLinuxMySQL(IP, user);
	            System.out.println("");
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            
	            System.out.println("[Bache]: Prepare for uninstall");
	            prepare_umount(IP, user, bcacheDevice);
	            Thread.sleep(1000);
	            
	            // Uninsall bcache
	            uninstallBcache(IP);
	            System.out.println("[Bache]: Uninstalled Bcache");
	            Thread.sleep(1000);

	            // Install bcache
	            installBcache(IP, cacheSettings.get("cachepartition").toString(), cacheSettings.get("diskpartition").toString(), cacheSettings.get("cacheblocksize").toString(), cacheSettings.get("writepolicy").toString());
	            Thread.sleep(1000);
	            System.out.println("[Bache]: Installed Bcache");
	            
	            // Copy data from backup to data directory
	            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	    		Date date = new Date();
	            System.out.println("Start Copy at:"+dateFormat.format(date));
	            CopyFromTo2Bcache(IP, user, backupFolder, mysqlDataLoc);
	            System.out.println("===================================");
	            Thread.sleep(1000);
	            
	            System.out.println("[Bache]: Starting MySQL");
	            startLinuxMySQL(IP, user);
	            System.out.println("[Bache]: Restore finished");
			} catch(NullPointerException ne) {
				System.out.println("[Bache]: Missing cache settings for Bcache");
				ne.printStackTrace(System.out);
				System.exit(0);
			} catch (Exception e) {
				e.printStackTrace(System.out);
				System.exit(0);
			}
		}
		
	    private static void prepare_umount(String IP, String User, String partition) {
	        System.out.println("Checking " + partition + " for any process to kill.");
	        PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

	        String cmd = " sudo fuser -c " + partition + " ";
	        printWriter.println(cmd + "\n");
	        System.out.println("Running command on [" + IP + "]: " + cmd);
	        // System.out.println("Adding: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.CommandFile, true);

	        printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
	        cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
	        printWriter.println(cmd + "\n");
	        // System.out.println("Running command on ["+IP+"]: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
	        List<String> outs = executeRuntimeReturn(BGConstants.SSHCommandFile, true);

	        if (outs.size() > 0) {
	            for (String processes : outs) {
	                processes = processes.trim();
	                String[] strs = processes.split("[ ]");
	                for (int i = 0; i < strs.length; i++) {
	                    if (strs[i].equals("") || strs[i].equals(" "))
	                        continue;
	                    killRemoteProcess(IP, User, strs[i]);
	                }
	            }

	        }

	        // TODO: Check Outputs, make sure that this happened
	    }

	    private static void killRemoteProcess(String IP, String User, String process) {
	        PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

	        String cmd = " sudo kill -9 " + process + " ";
	        printWriter.println(cmd + "\n");
	        System.out.println("Running command on [" + IP + "]: " + cmd);
	        // System.out.println("Adding: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.CommandFile, true);

	        printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
	        cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
	        printWriter.println(cmd + "\n");
	        // System.out.println("Running command on ["+IP+"]: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
	        executeRuntime(BGConstants.SSHCommandFile, true);

	        // TODO: Check Outputs, make sure that this happened
	    }

	    public static void executeRuntime(String cmd, boolean wait) {
			Process p;
			try {
				p = Runtime.getRuntime().exec(cmd);
				if (wait) {
					InputStream stdout = p.getInputStream();
					BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
					String line = "";
					while ((line = reader.readLine()) != null) {
						System.out.println("Commandout: " + line);
					}
					p.waitFor();
				} else
					Thread.sleep(5000);
			} catch (Exception e2) {
				e2.printStackTrace(System.out);
			}

		}

	    public static PrintWriter createBashFilePrintWriter(String FileName) {
			PrintWriter printWriter = null;
			try {

				File file = new File(FileName);
				file.createNewFile();
				printWriter = new PrintWriter(FileName);
				printWriter.println("#!/bin/bash\n\n");
			} catch (FileNotFoundException e) {
				e.printStackTrace(System.out);
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
			return printWriter;
		}

		private static void format(String IP, String User, String partition) {
	        PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

	        String cmd = " sudo mkfs /dev/" + partition + " -t ext4 ";
	        printWriter.println(cmd + "\n");
	        System.out.println("Running command on [" + IP + "]: " + cmd);
	        // System.out.println("Adding: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.CommandFile, true);

	        printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
	        cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
	        printWriter.println(cmd + "\n");
	        // System.out.println("Running command on ["+IP+"]: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
	        executeRuntime(BGConstants.SSHCommandFile, true);

	        // TODO: Check Outputs, make sure that this happened
	    }

	    public static void CopyFromTo2(String IP, String User, String src, String dist) {
	        PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

	        String cmd = " sudo cp -afrv " + src + "* " + dist + " ";
	        printWriter.println(cmd + "\n");
	        System.out.println("Running command on [" + IP + "]: " + cmd);
	        // System.out.println("Adding: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.CommandFile, true);

	        printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
	        cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
	        printWriter.println(cmd + "\n");
	        // System.out.println("Writing to BGConstants.SSHCommandFile: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
	        executeRuntime(BGConstants.SSHCommandFile, true);
	    }
	    public static void copyFromLinuxRemoteToLocal(String IP, String User, String src, String dist) {
	        PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

	        String cmd = " scp -r "+IP +":"+ src  + " " +dist + " ";
	        printWriter.println(cmd + "\n");
	        System.out.println("Running command on [" + IP + "]: " + cmd);
	        // System.out.println("Adding: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.CommandFile, true);

	        printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
	        cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
	        printWriter.println(cmd + "\n");
	        // System.out.println("Writing to BGConstants.SSHCommandFile: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
	        executeRuntime(BGConstants.CommandFile, true);
	    }



	    private static void dmsetup(String IP, String User, String cacheDev) {
	        PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

	        String cmd = " sudo dmsetup remove " + cacheDev + " ";
	        printWriter.println(cmd + "\n");
	        System.out.println("Running command on [" + IP + "]: " + cmd);
	        // System.out.println("Adding: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.CommandFile, true);

	        printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
	        cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
	        printWriter.println(cmd + "\n");

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
	        executeRuntime(BGConstants.SSHCommandFile, true);

	        // TODO: Check Outputs, make sure that this happened
	    }

	    private static void FlashcachePermissions(String IP, String User, String dir) {
	        PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

	        String cmd = " sudo chown -vR mysql:mysql " + dir + " ";
	        printWriter.println(cmd + "\n");
	        System.out.println("Running command on [" + IP + "]: " + cmd);
	        cmd = " sudo chmod -vR 700 " + dir + " ";
	        printWriter.println(cmd + "\n");
	        System.out.println("Running command on [" + IP + "]: " + cmd);
	        // System.out.println("Adding: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.CommandFile, true);

	        printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
	        cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
	        printWriter.println(cmd + "\n");
	        // System.out.println("Running command on ["+IP+"]: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
	        executeRuntime(BGConstants.SSHCommandFile, true);

	        // TODO: Check Outputs, make sure that this happened
	    }

	    private static void mount_dir(String IP, String User, String cacheDir, String ToBeMounted) {
	        PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

	        String cmd = " sudo mount " + cacheDir + " " + ToBeMounted + " ";
	        printWriter.println(cmd + "\n");
	        System.out.println("Running command on [" + IP + "]: " + cmd);
	        // System.out.println("Adding: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.CommandFile, true);

	        printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
	        cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
	        printWriter.println(cmd + "\n");
	        // System.out.println("Running command on ["+IP+"]: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
	        executeRuntime(BGConstants.SSHCommandFile, true);

	        // TODO: Check Outputs, make sure that this happened
	    }

	    private static void flashcache_create(String IP, String User, String cacheDir, String ToBeCached) {
	        PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);
	//-p back|around|thru 
	        String cmd = " sudo flashcache_create -p thru -b 4k cachedev " + cacheDir + " " + ToBeCached + " ";
	        printWriter.println(cmd + "\n");
	        System.out.println("Running command on [" + IP + "]: " + cmd);
	        // System.out.println("Adding: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.CommandFile, true);

	        printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
	        cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
	        printWriter.println(cmd + "\n");
	        // System.out.println("Running command on ["+IP+"]: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
	        executeRuntime(BGConstants.SSHCommandFile, true);

	        // TODO: Check Outputs, make sure that this happened
	    }

	    private static void destroy_flashcache(String IP, String User, String cacheDir) {
	        PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

	        String cmd = " sudo flashcache_destroy -f " + cacheDir + " ";
	        printWriter.println(cmd + "\n");
	        System.out.println("Running command on [" + IP + "]: " + cmd);
	        // System.out.println("Adding: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.CommandFile, true);

	        printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
	        cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
	        printWriter.println(cmd + "\n");
	        // System.out.println("Running command on ["+IP+"]: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
	        executeRuntime(BGConstants.SSHCommandFile, true);

	        // TODO: Check Outputs, make sure that this happened

	    }

	    private static void umount_dir(String IP, String User, String dir) {
	        PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

	        String cmd = " sudo umount " + dir + " ";
	        printWriter.println(cmd + "\n");
	        System.out.println("Running command on [" + IP + "]: " + cmd);
	        // System.out.println("Adding: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.CommandFile, true);

	        printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
	        cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
	        printWriter.println(cmd + "\n");
	        // System.out.println("Running command on ["+IP+"]: " + cmd);

	        printWriter.close();

	        executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
	        executeRuntime(BGConstants.SSHCommandFile, true);

	        // TODO: Check Outputs, make sure that this happened

	    }
	    public static void CreateDir(String dist) {
			PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

			String cmd = " mkdir -p " + dist + " ";
			printWriter.println(cmd + "\n");
			System.out.println("Adding: " + cmd);

			printWriter.close();

			executeRuntime("chmod +x " + BGConstants.CommandFile, true);
			executeRuntime(BGConstants.CommandFile, true);
		}

		public static void CreateRemoteDir(String IP, String User, String dist) {
			PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

			String cmd = " sudo mkdir -p " + dist + " ";
			printWriter.println(cmd + "\n");
			System.out.println("Running command on ["+IP+"]: " + cmd);
			//System.out.println("Adding: " + cmd);

			printWriter.close();

			executeRuntime("chmod +x " + BGConstants.CommandFile, true);

			printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
			cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
			printWriter.println(cmd + "\n");
			//System.out.println("Writing to BGConstants.SSHCommandFile: " + cmd);

			printWriter.close();
			
			executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
			executeRuntime(BGConstants.SSHCommandFile, true);
			//executeRuntime(CommandFile, true);
		}
		
		public static String fixPath(String path) {
			path = path.replaceAll("/{2,}", "/");
			return path;
		}
		

		public static String fixDir(String dist) {
			if (dist.charAt(dist.length() - 1) != '/')
				dist = dist + "/";
			dist = dist.replaceAll("/{2,}", "/");
			return dist;
		}
		
		public static void deleteRemoteDir(String IP, String User, String dist) {
			PrintWriter printWriter = createBashFilePrintWriter(BGConstants.CommandFile);

			String cmd = " sudo rm -rf " + dist + " ";
			printWriter.println(cmd + "\n");
			System.out.println("Running command on ["+IP+"]: " + cmd);
			//System.out.println("Adding: " + cmd);

			printWriter.close();

			executeRuntime("chmod +x " + BGConstants.CommandFile, true);

			printWriter = createBashFilePrintWriter(BGConstants.SSHCommandFile);
			cmd = "ssh " + User + "@" + IP + " 'bash -s ' < " + BGConstants.CommandFile;
			printWriter.println(cmd + "\n");
			//System.out.println("Writing to BGConstants.SSHCommandFile: " + cmd);
			printWriter.close();

			executeRuntime("chmod +x " + BGConstants.SSHCommandFile, true);
			executeRuntime(BGConstants.SSHCommandFile, true);
			
		
	    
		}    
		public static List<String> executeRuntimeReturn(String cmd, boolean wait) {
			Process p;
			List<String> outs = new ArrayList<String>();
			try {
				p = Runtime.getRuntime().exec(cmd);
				if (wait) {
					InputStream stdout = p.getInputStream();
					BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
					String line = "";
					while ((line = reader.readLine()) != null) {
						outs.add(line);
						System.out.println("Commandout: " + line);
					}
					p.waitFor();
				} else
					Thread.sleep(2000);
			} catch (IOException | InterruptedException e2) {
				e2.printStackTrace(System.out);
			}
			return outs;

		}

///////////////////////////////

	public static void runLinuxRemoteC(String ip, String cmd){
		Process p;
		String remoteSSHCmdFormat="";
		if (runningFromLinux){
			remoteSSHCmdFormat= linuxRemoteSSHCmdFormat;
		}
		else{
			remoteSSHCmdFormat=cygwinRemoteSSHCmdFormat;
		}
//		String cmd="C:/cygwin64/bin/ssh "+user+"@ "+ip+" \" killall sar \"";
//		executeRuntime(cmd,false);
		try {
			p = Runtime.getRuntime().exec(new String[]{"/bin/sh","-c",String.format(remoteSSHCmdFormat, ip, cmd)});
			Thread.sleep(1000);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		}
	}

	public static void runLinuxRemote(String ip, String cmd){
		String remoteSSHCmdFormat="";
		if (runningFromLinux){
			remoteSSHCmdFormat= linuxRemoteSSHCmdFormat;
		}
		else{
			remoteSSHCmdFormat=cygwinRemoteSSHCmdFormat;
		}
//		String cmd="C:/cygwin64/bin/ssh "+user+"@ "+ip+" \" killall sar \"";
//		executeRuntime(cmd,false);
		executeRuntime(new String[]{"/bin/sh","-c",String.format(remoteSSHCmdFormat, ip, cmd)},false);
	
		
	
		
	}

	public static void main(String[] args) {
		Utilities.manageCache("10.0.0.120", 11111, "start");

//		Utilities.stopLinuxMySQL("h1.exp28.KOSAR", "");
//
//		Utilities.GiveMePermission("h1.exp28.KOSAR", "", "/var/lib/mysql");
//
//		Utilities.DeleteAll("h1.exp28.KOSAR", "", "/var/lib/mysql", true);
//	
//		Utilities.copyFilesFromLinux("h1.exp28.KOSAR:"+"/var/lib/mysql/", "h1.exp28.KOSAR", "", "/proj/KOSAR/yaz/mysqlbackup/mysql/*", false);
//
//
//		Utilities.RemoveMyPermission("h1.exp28.KOSAR", "", "/var/lib/mysql");
//
//		Utilities.startLinuxMySQL("h1.exp28.KOSAR", "");
			}

	public static void restoreMySQLLinux(String IP, String mysqlDataLoc, String backupfolder){
try{
		String user="hieun";
		
		System.out.println("Restart MySQL");

		stopLinuxMySQL(IP, user);
        //	DD.restoreMySQLLocations(IP);
            System.out.println("");
            System.out.println("===================================");
            Thread.sleep(1000);
            System.out.println("");
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    		Date date = new Date();
            System.out.println("Start Copy at:"+dateFormat.format(date));
            executeRuntime("ssh "+ IP +" bash ~/rmvmysql.sh ", true);
            CopyFromTo2(IP, user, backupfolder, mysqlDataLoc);
            System.out.println("===================================");
            Thread.sleep(1000);
            System.out.println("");
            startLinuxMySQL(IP, user);
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }



	}

	private static void MKBackupOracle() {

		String Dist = "C:\\Documents and Settings\\Administrator\\Desktop\\DB2\\";

		String source = "C:\\app\\cosar\\oradata\\orcl\\";

		// RunMethod("shutdownOracle", "Shutdown", 1);

		// RunMethod("restoreDB", "DB Copy", 1);

		RunMethod("shutdownOracle", "Shutdown", 1, "", "");

		RunMethod("restoreTheWholeDB", "Copying DB Files", 1, source, Dist);

		RunMethod("startOracle", "Startup", 1, "", "");

		// RunMethod("startOracle", "Startup", 1);

	}



	public static void MKBackupMySQL(String type) {

		String src = "C:/mysql/data/";

		String dist = "C:/mysqlbackup/";

		if (type.equals("ycsb")) {

			src += "ycsb";

			dist += "ycsb";

		} else if (type.equals("tpcc")) {

			src += "tpcc";

			dist += "tpcc";

		}

		else if (type.equals("bg")) {

			src += "bg";

			dist += "bg";

		}

		if (type.equals("ycsb") || type.equals("tpcc") || type.equals("bg")) {

			RunMethod("shutdownMySQL", "Shutdown", 1, "", "");

			RunMethod("restoreTheWholeDB", "Copying DB Files", 1, src, dist);

			RunMethod("startMySQL", "Startup", 1, "", "");

		}

		else

			System.out.println("ERROR: Wrong type.");

	}



	private static void initNewExpermentOracle() {

		String source = "C:\\Documents and Settings\\Administrator\\Desktop\\DB2\\";

		String Dist = "C:\\app\\cosar\\oradata\\orcl\\";

		RunMethod("shutdownOracle", "Shutdown", 1, "", "");

		RunMethod("restoreTheWholeDB", "Copying DB Files", 1, source, Dist);

		RunMethod("startOracle", "Startup", 1, "", "");

	}



	private static void initNewExpermentMySQL(String type) {

			String dist = "C:/mysql/data/";

			String src = "C:/Users/MR1/Desktop/Backup/";

			if (type.equals("ycsb")) {

				src += "ycsb";

				dist += "ycsb";

			} else if (type.equals("tpcc")) {

				src += "tpcc";

				dist += "tpcc";

			}

			if (type.equals("ycsb") || type.equals("tpcc")) {

				RunMethod("shutdownMySQL", "Shutdown", 1, "", "");

				RunMethod("restoreTheWholeDB", "Copying DB Files", 1, src, dist);

				RunMethod("startMySQL", "Startup", 1, "", "");

			}

			else

				System.out.println("ERROR: Wrong type.");

		}



	private static boolean DeleteMySQLOldData(String ip, int listenerPort, String src) {

		Socket requestSocket;

		boolean output;

		String cmdMsg = src;

		boolean dataDeleted = false;

		int count = 0;

		try {

			while (!dataDeleted) {

				if (count >= 5) {

					RunMethod(Operation.shutdownMySQL, "Shutdown", 2);

					Thread.sleep(10000);

					count = 0;

				}

				requestSocket = new Socket(ip, listenerPort);

				output = Boolean.parseBoolean(sendMessage(requestSocket, cmdMsg, M_DELETE_CMD));

				Thread.sleep(5000);

				if (isDataDeleted(ip, listenerPort, src)) {

					dataDeleted = true;

					break;

				} else {

					System.out.println("Err: Data was not fully deleted. Trying again.");

					count++;

				}

			}

		} catch (UnknownHostException e) {

			e.printStackTrace(System.out);

			return false;

		} catch (IOException e) {

			e.printStackTrace(System.out);

			return false;

		} catch (InterruptedException e) {

			e.printStackTrace(System.out);

			return false;

		}

		return dataDeleted;

	}



	private static boolean isDataDeleted(String ip, int listenerPort, String src) {

		Socket requestSocket;

		String cmdMsg = src;

		try {

			requestSocket = new Socket(ip, listenerPort);

			boolean output = Boolean.parseBoolean(sendMessage(requestSocket, cmdMsg, M_DELETE_CHECK_CMD));

			return output;

		} catch (UnknownHostException e) {

			e.printStackTrace(System.out);

		} catch (IOException e) {

			e.printStackTrace(System.out);

		}

		return false;

	}



	private static boolean restoreTheWholeDB(String ip, int listenerPort, String src, String Dist) {



		Socket requestSocket;

		boolean output;

		// String src = "C:\\Documents and

		// Settings\\Administrator\\Desktop\\DB2\\";

		// String Dist = "C:\\Documents and

		// Settings\\Administrator\\Desktop\\Test\\";

		// String Dist = "C:\\app\\cosar\\oradata\\orcl\\";

		String cmdMsg = src + "|" + Dist;

		// String cmdMsg = "restoreDB.bat";

		try {

			requestSocket = new Socket(ip, listenerPort);

			output = Boolean.parseBoolean(sendMessage(requestSocket, cmdMsg, RESTORE2_DB_CMD));

			// return output;

		} catch (UnknownHostException e) {

			e.printStackTrace(System.out);

			return false;

		} catch (IOException e) {

			e.printStackTrace(System.out);

			return false;

		}

		return output;

	}



	private static boolean isDataRestored(String ip, int listenerPort, String src, String dist) {

		Socket requestSocket;

		String cmdMsg = src + "|" + dist;

		try {

			requestSocket = new Socket(ip, listenerPort);

			boolean output = Boolean.parseBoolean(sendMessage(requestSocket, cmdMsg, M_RESTORE_CHECK_CMD));

			return output;

		} catch (UnknownHostException e) {

			e.printStackTrace(System.out);

		} catch (IOException e) {

			e.printStackTrace(System.out);

		}

		return false;

	}





	private static boolean RestoreMySQLData(String ip, int listenerPort, String src, String dist) {

		Socket requestSocket;

		boolean output;

		String cmdMsg = src + "|" + dist;

		boolean dataRestored = false;

		try {

			while (!dataRestored) {

				requestSocket = new Socket(ip, listenerPort);

				output = Boolean.parseBoolean(sendMessage(requestSocket, cmdMsg, M_RESTORE_CMD));

				Thread.sleep(5000);

				if (isDataRestored(ip, listenerPort, src, dist)) {

					dataRestored = true;

					break;

				} else

					System.out.println("Err: Data was not fully restored. Trying again.");

			}

		} catch (UnknownHostException e) {

			e.printStackTrace(System.out);

			return false;

		} catch (IOException e) {

			e.printStackTrace(System.out);

			return false;

		} catch (InterruptedException e) {

			e.printStackTrace(System.out);

			return false;

		}

		return dataRestored;

	}



	private static void RunMethod(Operation MethodName, String text, int level, String... param) {

		String tab = "";

		for (int i = 0; i < level; i++)

			tab += "\t";

		Date started, ended;

		started = new Date();

		System.out.println(tab + text + " started: " + started);

		switch (MethodName) {

		case CountNewOrder:

			//CountNewOrder(param[0], (param[1].equals("Cache") ? "1" : "2"));

			break;

		case AverageThroughput:

			//AverageThroughput(param[0]);

			break;

		case BackupOracle:

			MKBackupOracle();

			break;

		case BackupMySQL:

			MKBackupMySQL(param[0]);

			break;

		case RestoreOracle:

			initNewExpermentOracle();

			break;

		case RestoreMySQL:

			initNewExpermentMySQL(param[0]);

			break;

		case shutdownOracle:

			shutdownOracle(dbipOracle, "-1", LPort);

			break;

		case shutdownMySQL:

			shutdownMySQL(dbipMySQL, "-1", LPort);

			break;

		case startOracle:

			startOracle(dbipOracle, "-1", LPort);

			break;

		case startMySQL:

			startMySQL(dbipMySQL, "-1", LPort);

			break;

		case CopyDBStats:

			CopyDBStats(param[0], param[1]);

			break;

		case restoreTheWholeDB:

			if (restoreTheWholeDB(dbipMySQL, LPort, param[0], param[1]))

				System.out.println(tab + "The files were copied.");

			else

				System.out.println(tab + "The files weren't copied.");

			break;

		case StartCache:

		//	startMemcache();

			break;

		case StopCache:

			//stopMemcache();

			break;

		case StartCacheStats:

			//startLinuxstats();

			break;

		case StopCacheStats:

			//stopLinuxstats(param[0]);

			break;

		case GraphesWithCache:

			//ResourcesFilesCache(param[0]);

			break;

		case Graphes:

			ResourcesFiles(param[0]);

			break;

		case AverageThroughputGraph:

			//AverageThroughputGraph(param[0]);

			break;

		case NewMySQLRestore:

			NewMySQLRestore(dbipMySQL, LPort, param[0], param[1]);

			break;

		case DeleteMySQLOldData:

			DeleteMySQLOldData(dbipMySQL, LPort, param[0]);

			break;

		case RestoreMySQLData:

			RestoreMySQLData(dbipMySQL, LPort, param[0], param[1]);

			break;

		case CalculateResults:

			//ResultWriter(param[0], Integer.parseInt(param[1]), Integer.parseInt(param[2]),

				//	Boolean.parseBoolean(param[3]));

			break;

		}

		ended = new Date();

		System.out.print(tab + text + " ended: " + ended);

		long diff = ended.getTime() - started.getTime();

		long diffSeconds = diff / 1000 % 60;

		long diffMinutes = diff / (60 * 1000) % 60;

		long diffHours = diff / (60 * 60 * 1000) % 24;

		System.out.println(" (Duration: "

				+ String.format("%02d:%02d:%02d.%03d)", diffHours, diffMinutes, diffSeconds, (diff % 1000)));

		System.out.println(tab + "=======================================");



	}



	public static void shutdownMySQL(String ip, String port, int listenerPort) {

		System.out.println("Shutting down MySQL ....");

		Socket requestSocket;

		// String cmdMsg = "oradim -shutdown -sid orcl";

		String cmdMsg = "C:/mysql/bin/mysqladmin -u cosar --password=gocosar shutdown";

		try {

			int i = 0;

			boolean serverStopped = false;

			while (i < numAttepmts) {

				requestSocket = new Socket(ip, listenerPort);

				String output = sendMessage(requestSocket, cmdMsg, NORMAL_CMD);

				if (isMYSQLServerRunning(ip, port, listenerPort)) {

					i++;

					Thread.sleep(5000);

					continue;

				}

				serverStopped = true;

				break;

			}

			if (!serverStopped) {

				System.out.println("Err: The server is not shutting down. Exiting...");

				System.exit(0);

			}

			/*

			 * cmdMsg = "lsnrctl stop"; requestSocket = new Socket(ip,

			 * listenerPort); String output = sendMessage(requestSocket, cmdMsg,

			 * NORMAL_CMD);

			 */

			Thread.sleep(5000);

			System.out.println("server is shut down.");

		} catch (Exception e) {

			e.printStackTrace(System.out);

		}



	}



		 private static void DeleteAll(String IP, String User, String dist, boolean DBFiles) {

	        if (dist.charAt(dist.length() - 1) != '/')

	            dist = dist + '/';



	        String cmd = " rm -f -r "+dist+"*";

	       




	      



	        

	        cmd=" ssh -o StrictHostKeyChecking=no " +IP+ " "+cmd; 

System.out.println(cmd);	        

	        

	        executeRuntime(new String[]{"/bin/sh","-c",cmd}, false);



	        try {

	            Thread.sleep(1000);

	        } catch (InterruptedException e) {

	            e.printStackTrace(System.out);

	        }



	        

	        

	    }



	    private static boolean isDirEmpty(String IP, String User, String dir) {

	       



	        String cmd = " find " + dir + " -mindepth 1 -empty -type d | wc -l ";

	        

	        System.out.println("Running: " + cmd);







	        cmd=cmd+"| ssh -o StrictHostKeyChecking=no" +IP+ "/bin/bash"; 

	        

	        

	        List<String> outs = executeRuntimeReturn(cmd, true);

	        for (String line : outs) {

	            int i = -1;

	            try {

	                i = Integer.parseInt(line);

	            } catch (Exception e) {
			System.out.println("Error");



	            }



	            if (i == -1)

	                continue;



	            if (i > 0)

	                return false;

	            else if (i == 0)

	                return true;

	        }



	        return false;

	    }

	 

	    private static void RemoveMyPermission(String IP, String User, String dist) {

	        



	        String cmd = " sudo chown -R mysql:root " + dist + " ";

	        




	       



	        cmd="ssh -o StrictHostKeyChecking=no " +IP+ " "+cmd; 

	        
System.out.println(cmd);
	        

	        executeRuntime(new String[]{"/bin/sh","-c",cmd}, false);



	        // TODO: Check Permission, make sure that this happened

	    }

	  		  
	    private static void GiveMePermission(String IP, String User, String dist) {

	        



	        String cmd = " sudo chmod -R 777 " + dist + " ";

	        


	        cmd="ssh -o StrictHostKeyChecking=no " +IP+ " "+cmd; 

	        
System.out.println(cmd);
	        

	        executeRuntime(new String[]{"/bin/sh","-c",cmd}, false);

	       



	        

	    



	        

	    }

	    private static List<String> getFile(String file) {

	        File f = new File(file);

	        if (!f.exists()) {

	            System.out.println("ERROR: File \"" + file + "\" not found.");

	            System.exit(1);

	        }

	        BufferedReader reader = null;

	        List<String> lines = new ArrayList<String>();

	        try {

	            reader = new BufferedReader(new FileReader(file));

	            String text = null;

	            int index = 0;

	            while ((text = reader.readLine()) != null) {

	                lines.add(index, text);

	                index++;

	            }

	        } catch (FileNotFoundException e) {

	            e.printStackTrace(System.out);

	        } catch (IOException e) {

	            e.printStackTrace(System.out);

	        } finally {

	            try {

	                if (reader != null) {

	                    reader.close();

	                }

	            } catch (IOException e) {

	                e.printStackTrace(System.out);

	            }

	        }

	        return lines;

	    }

	

	

	

	public static void NewMySQLRestore(String ip, int listenerPort, String src, String dist) {

		try {

			RunMethod(Operation.shutdownMySQL, "Shutdown", 1);

			Thread.sleep(5000);

			RunMethod(Operation.DeleteMySQLOldData, "Deleting MySQL Old Data", 1, dist);

			Thread.sleep(5000);

			RunMethod(Operation.RestoreMySQLData, "Restoring MySQL Data", 1, src, dist);

			Thread.sleep(5000);

			RunMethod(Operation.startMySQL, "Startup", 1);

		} catch (InterruptedException e) {

			e.printStackTrace(System.out);

		}

	}

	public enum Operation {

		Check("Check"),



		BackupOracle("BackupOracle"), RestoreOracle("RestoreOracle"),



		BackupMySQL("BackupMySQL"), RestoreMySQL("RestoreMySQL"),



		startOracle("startOracle"), shutdownOracle("shutdownOracle"),



		startMySQL("startMySQL"), shutdownMySQL("shutdownMySQL"),



		StartCache("StartCache"), StopCache("StopCache"),



		StartCacheStats("StartCacheStats"), StopCacheStats("StopCacheStats"),



		CopyDBStats("CopyDBStats"), Graphes("Graphes"), GraphesWithCache("GraphesWithCache"),



		restoreTheWholeDB("restoreTheWholeDB"), NewMySQLRestore("NewMySQLRestore"), ERROR("ERROR"), CopyCOErrors(

				"CopyCOErrors"), CalculateResults("CalculateResults"), AverageThroughputGraph("AverageThroughputGraph"),



		CountNewOrder("CountNewOrder"), AverageThroughput("AverageThroughput"), DeleteMySQLOldData(

				"DeleteMySQLOldData"), RestoreMySQLData("RestoreMySQLData");



		public final String text;



		Operation(String t) {

			text = t;

		}



		public static Operation getOperation(String OpText) {

			for (Operation op : Operation.values()) {

				if (op.equals(restoreTheWholeDB))

					continue;

				if (op.text.equals(OpText))

					return op;

			}

			return Operation.ERROR;

		}

	}



	private static void RunMethod(String MethodName, String text, int level, String sourse, String Dist) {

		String tab = "";

		for (int i = 0; i < level; i++)

			tab += "\t";

		Date started, ended;

		started = new Date();

		System.out.println(tab + text + " started: " + started);

		switch (MethodName) {

		case "BackupOracle":

			MKBackupOracle();

			break;

		case "BackupMySQL":

			MKBackupMySQL(sourse);

			break;

		case "initOracle":

			initNewExpermentOracle();

			break;

		case "initMySQL":

			initNewExpermentMySQL(sourse);

			break;

		case "shutdownOracle":

			shutdownOracle(dbipOracle, "-1", LPort);

			break;

		case "shutdownMySQL":

			shutdownMySQL(dbipMySQL, "-1", LPort);

			break;

		case "startOracle":

			startOracle(dbipOracle, "-1", LPort);

			break;

		case "startMySQL":

			startMySQL(dbipMySQL, "-1", LPort);

			break;

		case "CopyDBStats":

			CopyDBStats(sourse, Dist);

			break;

		case "ResourcesFiles":

			ResourcesFiles(sourse);

			break;

		case "restoreTheWholeDB":

			if (restoreTheWholeDB(dbipMySQL, "-1", LPort, sourse, Dist))

				System.out.println(tab + "The files were copied.");

			else

				System.out.println(tab + "The files weren't copied.");

			break;

		}

		ended = new Date();

		System.out.print(tab + text + " ended: " + ended);

		long diff = ended.getTime() - started.getTime();

		long diffSeconds = diff / 1000 % 60;

		long diffMinutes = diff / (60 * 1000) % 60;

		long diffHours = diff / (60 * 60 * 1000) % 24;

		System.out.println(" (Duration: "

				+ String.format("%02d:%02d:%02d.%03d)", diffHours, diffMinutes, diffSeconds, (diff % 1000)));

		System.out.println(tab + "=======================================");



	}



	private static void ResourcesFiles(String Path) {

		Vector<StringBuilder> sv = createFiles(Path, "Worker");

		createResourcesFiles(sv, Path, "W");

		Vector<StringBuilder> sv2 = createFiles(Path, "DB");

		createResourcesFiles(sv2, Path, "D");



		createCharts(Path, "W");

		createCharts(Path, "D");

	}



	public static void createCharts(String filespath, String delimeter) {

		// TODO Auto-generated method stub

		String pythonscriptpath = "C:\\Users\\MR1\\Desktop\\BG\\python-plots\\scripts\\camp\\";

		String cmd = "C:\\Python27\\python " + pythonscriptpath + "admCntrl.py " + filespath + "\\ " + delimeter;

		try {

			Process p = Runtime.getRuntime().exec(cmd);

			InputStream out = p.getInputStream();

			BufferedReader reader = new BufferedReader(new InputStreamReader(out));

			String line = "";

			// Thread.sleep(60000);

			// p.destroy();

			// while ((line = reader.readLine ()) != null )

			// {

			//

			//

			// System.out.println ("Pythonout: "+ line);

			// }

			// p.waitFor();



		} catch (Exception e) {

			// TODO Auto-generated catch block

			e.printStackTrace(System.out);

		}



	}



	public static Vector<StringBuilder> createFiles(String path, String fileNamePattern) {

		Vector<StringBuilder> sv = new Vector<StringBuilder>();

		// Vector<File> filesVector=new Vector<File>();

		File srcFolder = new File(path);

		String files[] = srcFolder.list();

		for (String file : files) {

			File srcFile = new File(srcFolder, file);

			if (!srcFile.isDirectory() && srcFile.getName().contains(fileNamePattern)) {

				// filesVector.add(srcFile);

				StringBuilder s = new StringBuilder();

				readFile(srcFile, s);

				sv.add(s);

			}

		}

		return sv;

	}



	private static void readFile(File srcFile, StringBuilder s) {

		// TODO Auto-generated method stub



		FileInputStream fis = null;

		try {

			fis = new FileInputStream(srcFile);

		} catch (FileNotFoundException e) {

			// TODO Auto-generated catch block

			e.printStackTrace(System.out);

		}



		// Construct BufferedReader from InputStreamReader

		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		String newline = System.getProperty("line.separator");

		String line = null;

		try {

			while ((line = br.readLine()) != null) {

				if (line.contains("OS="))

					s.append(line + newline);



			}

		} catch (IOException e) {

			// TODO Auto-generated catch block

			e.printStackTrace(System.out);

		}

		try {

			br.close();

		} catch (IOException e) {

			// TODO Auto-generated catch block

			e.printStackTrace(System.out);

		}

		// System.out.println("done");

	}

	public static void startAnticacheCluster(String hosts, int numberPartitionsPerHost, String antiCacheFolder) {

		// TODO Auto-generated method stub

		String remoteSSHCmdFormat="";

		if (runningFromLinux){

			remoteSSHCmdFormat= linuxRemoteSSHCmdFormat;

		}

		else{

			remoteSSHCmdFormat=cygwinRemoteSSHCmdFormat;

		}

		

		String ips[]=hosts.split(",");

	

		String hostsString="";

		int siteId=0;

		int i=0;

		

		for ( String ip:ips){

			boolean firstTime=true;

			String partitions="";

			while (i% numberPartitionsPerHost!=0 || firstTime){

				

				firstTime=false;

				partitions=partitions+i+',';

				i++;

			}

			partitions=partitions.substring(0, partitions.length()-1);

			

			hostsString=hostsString+ip+":"+siteId+":"+partitions+';';

					siteId++;

			

		}

		hostsString=hostsString.substring(0,hostsString.length()-1);

		//String hostsString=ip+":0:0-1" ;

		int memorythreshold=14000;

		//-Devictable=\"Users,Friendship,Modify,Resource\"

		String anticachepreparecmd=" cd "+antiCacheFolder+"/h-store/ && ant hstore-prepare -Dhosts=\""+hostsString+ "\" -Dproject=example -Devictable=\"Users,Friendship\" " ;

		String anticacheRuncmd=" cd "+antiCacheFolder+"/h-store/ && ant hstore-benchmark -Dproject=example -Dnoloader=false -Dnoshutdown=true -Dnodataload=false -Dnoexecute=true -Dsite.jvm_asserts=false -Dsite.snapshot=true -Dsite.memory=10240 -Dsite.anticache_check_interval=5000 -Dsite.anticache_threshold_mb="+memorythreshold+" -Dsite.anticache_dir=obj/anticache -Dsite.anticache_enable=true " ;

		//String anticacheRuncmd=" cd git/h-store/ && ant hstore-benchmark -Dproject=example -Dnoloader=true -Dnoshutdown=true -Dnodataload=true -Dnoexecute=true" ;

		// ant hstore-benchmark -Dproject=example -Dhosts=10.0.0.175:0:0-1 -Dnoloader=true -Dnoshutdown=true -Dnodataload=true -Dnoexecute=true

	//	ant hstore-benchmark -Dproject=tpcc -Dsite.anticache_enable=true -Dsite.anticache_dir=obj/anticache

		String killcmd="killall java";//"pkill -9 -f hstore";

		System.out.println("Prepare Command:"+anticachepreparecmd);

		Utilities.executeRuntime(String.format(remoteSSHCmdFormat, ips[0], killcmd), false);

		Utilities.executeRuntime(String.format(remoteSSHCmdFormat, ips[0], anticachepreparecmd), false);

		String copyCmd="";

		for ( i=1 ; i<ips.length;i++){

			copyCmd=" scp "+ips[0]+":"+antiCacheFolder+"/h-store/example.jar " +ips[i]+":"+antiCacheFolder+"/h-store";

			Utilities.executeRuntime(copyCmd,false);

			

		}

		

		

		

		

		

		

		System.out.println("Run Command:"+anticacheRuncmd);



		HStoreProcess hstoreProcess= new HStoreProcess(String.format(remoteSSHCmdFormat, ips[0], anticacheRuncmd), "H-Store");

	

		hstoreProcess.start();

		

	}

	public static void stopAnticacheCluster(String hosts) {

		// TODO Auto-generated method stub

		String remoteSSHCmdFormat="";

		if (runningFromLinux){

			remoteSSHCmdFormat= linuxRemoteSSHCmdFormat;

		}

		else{

			remoteSSHCmdFormat=cygwinRemoteSSHCmdFormat;

		}

		antiCacheInit=false;

		String killcmd="killall java";

		String []ips=hosts.split(",");

		for (String ip: ips){

		Utilities.executeRuntime(String.format(remoteSSHCmdFormat, ip, killcmd), false);

		}

		

		

		

	}



	public static void createResourcesFiles(Vector<StringBuilder> sv, String path, String delimeter) {



		// TODO Auto-generated method stub

		int index;

		String[] lines;

		String cpu = "", net = "", mem = "", disk = "";

		try {



			PrintWriter cpuwriter = new PrintWriter(path + "\\" + delimeter + "cpu.txt", "UTF-8");

			PrintWriter memwriter = new PrintWriter(path + "\\" + delimeter + "mem.txt", "UTF-8");

			PrintWriter netwriter = new PrintWriter(path + "\\" + delimeter + "net.txt", "UTF-8");

			PrintWriter diskwriter = new PrintWriter(path + "\\" + delimeter + "disk.txt", "UTF-8");



			String newline = System.getProperty("line.separator");

			String[][] ss = new String[sv.size()][];

			String line = "";

			int minl = Integer.MAX_VALUE;

			for (int i = 0; i < sv.size(); i++) {

				ss[i] = sv.get(i).toString().split(newline);

				if (ss[i].length < minl)

					minl = ss[i].length;



			}

			sv.clear();

			for (int i = 0; i < minl; i++) {

				line = "";

				for (int j = 0; j < ss.length; j++) {

					line = line + ss[j][i] + "#";



				}

				lines = line.split("#");

				for (String l : lines) {

					cpu = cpu + l.substring(l.indexOf(':') + 1, l.indexOf(',')) + ",";

					l = l.substring(l.indexOf(',') + 1);

					mem = mem + l.substring(l.indexOf(':') + 1, l.indexOf(',')) + ",";

					l = l.substring(l.indexOf(',') + 1);

					net = net + l.substring(l.indexOf(':') + 1, l.indexOf(',')) + ",";

					l = l.substring(l.indexOf(',') + 1);

					disk = disk + l.substring(l.indexOf(':') + 1, l.indexOf(',')) + ",";



				}

				cpuwriter.println(cpu);

				netwriter.println(net);

				memwriter.println(mem);

				diskwriter.println(disk);

				cpu = "";

				net = "";

				mem = "";

				disk = "";



			}

			cpuwriter.close();

			netwriter.close();

			memwriter.close();

			diskwriter.close();



		} catch (Exception e) {

			// TODO Auto-generated catch block

			e.printStackTrace(System.out);

		}

	}



	private static void CopyDBStats(String sourse, String dist) {

		InputStream inStream = null;

		OutputStream outStream = null;



		try {



			File srcFolder = new File(sourse);

			File distFolder = new File(dist);

			if (!distFolder.exists())

				distFolder.mkdir();



			if (dist.charAt(dist.length() - 1) != '\\')

				dist += '\\';



			for (File srcFile : srcFolder.listFiles()) {



				inStream = new FileInputStream(srcFile);

				File distFile = new File(dist + srcFile.getName());

				outStream = new FileOutputStream(distFile);



				byte[] buffer = new byte[1024];



				int length;

				// copy the file content in bytes

				while ((length = inStream.read(buffer)) > 0) {



					outStream.write(buffer, 0, length);



				}



				inStream.close();

				outStream.close();



				// delete the original file

				srcFile.delete();



				System.out.println("File is copied successful!");

			}



		} catch (IOException e) {

			e.printStackTrace(System.out);

		}

	}

	public static void killCache(String ip){

		String cmd="C:/cygwin64/bin/ssh "+sshNoCeck+" yaz@"+ip+" \" killall twemcache \"";

		executeRuntime(cmd,false);

	}

	public static void manageCache(String ip,int listenerPort,String message) {
		try {

			Socket socket= new Socket(ip, listenerPort);

			OutputStream out = socket.getOutputStream();

			DataOutputStream os = new DataOutputStream(new BufferedOutputStream(out));

			os.writeBytes("memch" + " ");		

			os.writeInt(message.length());

			os.writeBytes(message);

			os.flush();			

			DataInputStream is = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

			is.readInt();

			socket.close();

		} catch (IOException e) {

			// TODO Auto-generated catch block

			e.printStackTrace(System.out);

		}

		

		

	}

	

	

	public static void executeRuntime(String[] cmd, boolean executeAndResturn){

		Process p;

		try {

			//"C:/cygwin64/bin/ssh mr1@10.0.0.150 \"sar -u 10 > cacheosstats/cpu.txt & \""

			p = Runtime.getRuntime().exec(cmd);

			//_schemaProcess = Runtime.getRuntime().exec("C:/cygwin64/bin/ssh mr1@10.0.0.150 \"killall sar\"");

			if (!executeAndResturn){

			InputStream stdout = p.getInputStream();

			BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));

			String line = "";

			while ((line = reader.readLine()) != null) {

				System.out.println("Commandout: " + line);

			}

			p.waitFor();

			}

			if (executeAndResturn){

				new PrintThread(p).start();

				

					

				

			}

		} catch (IOException | InterruptedException e2) {

			e2.printStackTrace(System.out);

		}

		

		

	}

		public static void collectLinuxstats(String dist,String ip){

	

		

		String cmd="pscp -r -pw Yy567999 "+ip+":/home/yaz/cacheosstats/*.txt "+ dist;//  ;

		executeRuntime(cmd,false);

	}

	

	public static void stopLinuxstats(String dist,String ip, String user, String statsLoc){

		String remoteSSHCmdFormat="";

		if (runningFromLinux){

			remoteSSHCmdFormat= linuxRemoteSSHCmdFormat;

		}

		else{

			remoteSSHCmdFormat=cygwinRemoteSSHCmdFormat;

		}

//		String cmd="C:/cygwin64/bin/ssh "+user+"@ "+ip+" \" killall sar \"";

//		executeRuntime(cmd,false);

		String cmd="killall sar";

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);

		

		if (!runningFromLinux){

		String pass="Yy567999";

		if (user.equals("mr1")){

			pass="Aa123456";

		}

		cmd="pscp -r -pw "+ pass+"  "+user+"@"+ip+":"+statsLoc+"/*.txt "+ dist;//  ;

		executeRuntime(cmd,false);

		}

		else

		{

			 	cmd=" scp "+sshNoCeck+" -r "+ip+":"+ statsLoc+"/*.txt "+ dist;

			 	

				executeRuntime(new String[]{"/bin/sh","-c",cmd},false);

		}

		

//		cmd="C:/cygwin64/bin/ssh "+user+"@"+ip+" \" cd cacheosstats && rm * \"";

//		executeRuntime(cmd,false);

		 cmd="cd "+statsLoc+" && rm * ";

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);

	}

	public static boolean startLinuxstats(String ip, String namePattern,String user, String location){

//		

//		String cpuCommand= " sar -u 10 | awk '{ if ($9 ~ /[0-9]/) {sum=100-$9; print sum  }}'";

//		String netCommand="  sar -n DEV 10 | awk '/bond0/{ sum= ($6+$7)/1024; print sum}' ";

//		String memCommand=" sar -r 10 | awk '{ if ($3 ~ /[0-9]/) {sum=$3/1024; print sum}}'";

//		String diskCommand="sar -d 10 | awk '/dev8/{ print $8}'";

		String remoteSSHCmdFormat="";

		if (runningFromLinux){

			remoteSSHCmdFormat= linuxRemoteSSHCmdFormat;

		}

		else{

			remoteSSHCmdFormat=cygwinRemoteSSHCmdFormat;

		}

		String cpuCommand= " sar -P ALL 10 ";

		String netCommand="  sar -n DEV 10  ";

		String memCommand=" sar -r 10 ";

		String diskCommand="sar -d 10 ";

//		String cmd="C:/cygwin64/bin/ssh "+user+"@"+ip+" \"killall sar \"";

//

//		executeRuntime(cmd,false);

		String cmd="killall sar";

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);

		 cmd="mkdir -p "+ location;

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);

		

//		cmd="C:/cygwin64/bin/ssh "+user+"@"+ip+" \""+cpuCommand+" > cacheosstats/1"+namePattern+"cpu.txt & \"";

//		executeRuntime(cmd,false);

		 cmd=cpuCommand+" > "+location+"/1"+namePattern+"cpu.txt &";

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);

		

//		cmd="C:/cygwin64/bin/ssh  "+user+"@"+ip+" \""+netCommand+" > cacheosstats/1"+namePattern+"net.txt & \"";

//		executeRuntime(cmd,false);

		cmd=netCommand+" > "+location+"/1"+namePattern+"net.txt &";

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);



//		cmd="C:/cygwin64/bin/ssh  "+user+"@"+ip+" \""+diskCommand+" > cacheosstats/1"+namePattern+"disk.txt & \"";

//		executeRuntime(cmd,false);

		

		cmd=diskCommand+" > "+location+"/1"+namePattern+"disk.txt &";

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);

		

//		cmd="C:/cygwin64/bin/ssh  "+user+"@"+ip+" \""+memCommand+" > cacheosstats/1"+namePattern+"mem.txt & \"";

//		executeRuntime(cmd,false);

		cmd=memCommand+" > "+location+"/1"+namePattern+"mem.txt &";

		executeRuntime(String.format(remoteSSHCmdFormat, ip, cmd),false);

		

		return true;

		

	}

	public static boolean restoreTheWholeDB(String ip, String port, int listenerPort, String src, String Dist) {

		RunMethod("shutdownMySQL", "Shutdown", 1, "", "");

		try {

			System.out.println("Wait for DBMS to completely shutdown...");

			Thread.sleep(5000);

		} catch (InterruptedException e1) {

			// TODO Auto-generated catch block

			e1.printStackTrace(System.out);

		}

		Socket requestSocket;

		boolean output;

		// String src = "C:\\Documents and

		// Settings\\Administrator\\Desktop\\DB2\\";

		// String Dist = "C:\\Documents and

		// Settings\\Administrator\\Desktop\\Test\\";

		// String Dist = "C:\\app\\cosar\\oradata\\orcl\\";

		String cmdMsg = src + "|" + Dist;

		// String cmdMsg = "restoreDB.bat";

		try {

			requestSocket = new Socket(ip, listenerPort);

			output = Boolean.parseBoolean(sendMessage(requestSocket, cmdMsg, RESTORE2_DB_CMD));

			// return output;

		} catch (UnknownHostException e) {

			e.printStackTrace(System.out);

			return false;

		} catch (IOException e) {

			e.printStackTrace(System.out);

			return false;

		}

		RunMethod("startMySQL", "Startup", 1, "", "");

		return output;

	}



	private static boolean restoreDB(String ip, String port, int listenerPort) {



		Socket requestSocket;

		boolean output;

		String source = "C:\\Documents and Settings\\Administrator\\Desktop\\DBBackup\\USERS01.DBF";

		String Dist = "C:\\app\\cosar\\oradata\\orcl\\USERS01.DBF";

		String cmdMsg = source + "|" + Dist;

		// String cmdMsg = "restoreDB.bat";

		try {

			requestSocket = new Socket(ip, listenerPort);

			output = Boolean.parseBoolean(sendMessage(requestSocket, cmdMsg, RESTORE_DB_CMD));

			// return output;

		} catch (UnknownHostException e) {

			e.printStackTrace(System.out);

			return false;

		} catch (IOException e) {

			e.printStackTrace(System.out);

			return false;

		}

		return output;



		/*

		 * String srcfolder = "\\\\10.0.0.65" + "\\DBBackup\\USERS01.DBF";

		 * String destfolder = "\\\\10.0.0.65" + "\\orcl\\USERS01.MR1";

		 * 

		 * File srcFile = new File(srcfolder); File destFile = new

		 * File(destfolder); try { InputStream in = new

		 * FileInputStream(srcFile); OutputStream out = new

		 * FileOutputStream(destFile); byte[] buffer = new byte[1024]; int

		 * length; // copy the file content in bytes while ((length =

		 * in.read(buffer)) > 0) { out.write(buffer, 0, length); }

		 * 

		 * in.close(); out.close(); System.out.println("File copied from " +

		 * srcfolder + " to " + destfolder); } catch (FileNotFoundException

		 * fnfe) { System.out.println(fnfe.getMessage()); } catch (IOException

		 * ioe) { System.out.println(ioe.getMessage()); }

		 */

	}



	public static void startOracle(String ip, String port, int listenerPort) {

		Socket requestSocket;

		// String cmdMsg = "oradim -startup -sid orcl";

		String cmdMsg = "sqlplus /nolog < startOracle.txt";



		try {

			int cnt = 0;

			boolean serverStarted = false;

			while (cnt < numAttepmts) {

				requestSocket = new Socket(ip, listenerPort);

				String output = sendMessage(requestSocket, cmdMsg, NORMAL_CMD);

				if (isORACLEServerRunning(ip, port, listenerPort)) {

					serverStarted = true;

					break;

				} else

					cnt++;

			}

			if (!serverStarted) {

				System.out.println("Err: Server did not start. Exiting...");

				System.exit(0);

			}

			/*

			 * cmdMsg = "lsnrctl start"; requestSocket = new Socket(ip,

			 * listenerPort); String output = sendMessage(requestSocket, cmdMsg,

			 * NORMAL_CMD);

			 */

			System.out.println("Shard0 " + " started.");

		} catch (Exception e) {

			e.printStackTrace(System.out);

		}

	}



	public static void shutdownOracle(String ip, String port, int listenerPort) {

		System.out.println("Shuttingdown node ....");

		Socket requestSocket;

		// String cmdMsg = "oradim -shutdown -sid orcl";

		String cmdMsg = "sqlplus /nolog < shutdownOracle.txt";

		try {

			int i = 0;

			boolean serverStopped = false;

			while (i < numAttepmts) {

				requestSocket = new Socket(ip, listenerPort);

				String output = sendMessage(requestSocket, cmdMsg, NORMAL_CMD);

				if (isORACLEServerRunning(ip, port, listenerPort)) {

					i++;

					Thread.sleep(5000);

					continue;

				}

				serverStopped = true;

				break;

			}

			if (!serverStopped) {

				System.out.println("Err: The server is not shutting down. Exiting...");

				System.exit(0);

			}

			/*

			 * cmdMsg = "lsnrctl stop"; requestSocket = new Socket(ip,

			 * listenerPort); String output = sendMessage(requestSocket, cmdMsg,

			 * NORMAL_CMD);

			 */

			System.out.println("server quit.");

		} catch (Exception e) {

			e.printStackTrace(System.out);

		}



	}



	public static void startMySQL(String ip, String port, int listenerPort) {

		System.out.println("Starting up MySQL ....");

		Socket requestSocket;

		// String cmdMsg = "oradim -startup -sid orcl";

		String cmdMsg = "C:/mysql/bin/mysqld";



		try {

			int cnt = 0;

			boolean serverStarted = false;

			while (cnt < numAttepmts) {

				requestSocket = new Socket(ip, listenerPort);

				String output = sendMessage(requestSocket, cmdMsg, NORMAL_CMD);

				//Thread.sleep(5000);

				if (isMYSQLServerRunning(ip, port, listenerPort)) {

					serverStarted = true;

					break;

				} else

					cnt++;

			}

			if (!serverStarted) {

				System.out.println("Err: Server did not start. Exiting...");

				System.exit(0);

			}

			/*

			 * cmdMsg = "lsnrctl start"; requestSocket = new Socket(ip,

			 * listenerPort); String output = sendMessage(requestSocket, cmdMsg,

			 * NORMAL_CMD);

			 */

			System.out.println("MySQL started.");

		} catch (Exception e) {

			e.printStackTrace(System.out);

		}

	}



	

	public static String sendMessage(Socket requestSocket, String message, String opType) throws IOException {

		OutputStream out = requestSocket.getOutputStream();

		DataOutputStream os = new DataOutputStream(new BufferedOutputStream(out));

		os.writeBytes(opType + " ");

		os.writeInt(message.length());

		os.writeBytes(message);

		os.flush();



		DataInputStream is = new DataInputStream(new BufferedInputStream(requestSocket.getInputStream()));

		String line = "";

		String response = "0";

		BufferedReader bri = new BufferedReader(new InputStreamReader(is));

		if (opType.equals(FOLDER_AVAILABILITY_CMD)) {

			while ((line = bri.readLine()) != null) {



				if (line.equals(folderNotAvailableMsg)) {

					response = folderNotAvailableMsg;

					System.out.println("Folder doesn't exist!");

				}

			}

		} else if (opType.equals(FOLDER_COPY_CMD)) {

			while ((line = bri.readLine()) != null) {

				if (line.equals("false")) {

					response = copyingFailedMsg;

					System.out.println("copying folder failed!");

				}

			}

		} else if (opType.equals(MYSQL_RUN_CMD)) {

			response = "";

			while ((line = bri.readLine()) != null) {

				response += line;

			}

		} else if (opType.equals(ORACLE_RUN_CMD)) {

			response = "";

			while ((line = bri.readLine()) != null) {

				response = line;

			}

		} 

		

		else if( opType.equals("memch")){

			response=Integer.toString(is.readInt());

		}

		else { // just wait for the output

			while ((line = bri.readLine()) != null)

				response = line;

		}

		/*

		 * int response = is.readInt(); System.out.println(response);

		 * if(response == 0) { // Error }

		 */

		is.close();

		os.close();

		out.close();

		requestSocket.close();

		return response;

	}



	public static boolean isORACLEServerRunning(String ip, String port, int listenerPort) {

		Socket requestSocket;

		// String cmdMsg = "sqlplus benchmark/111111@localhost:" + port +

		// "/ORCL < exitsqlplus.txt";

		String cmdMsg = "sqlplus /nolog < checkOracle.txt";

		try {

			requestSocket = new Socket(ip, listenerPort);

			boolean output = Boolean.parseBoolean(sendMessage(requestSocket, cmdMsg, ORACLE_RUN_CMD));

			return output;

		} catch (UnknownHostException e) {

			e.printStackTrace(System.out);

		} catch (IOException e) {

			e.printStackTrace(System.out);

		}

		return false;

	}



	/*public static boolean isMySQLServerRunning(String ip, String port, int listenerPort) {

		Socket requestSocket;

		// String cmdMsg = "sqlplus benchmark/111111@localhost:" + port +

		// "/ORCL < exitsqlplus.txt";

		String cmdMsg = "mysqladmin -u cosar --password=gocosar status";

		try {

			requestSocket = new Socket(ip, listenerPort);

			boolean output = Boolean.parseBoolean(sendMessage(requestSocket, cmdMsg, MYSQL_RUN_CMD));

			return output;

		} catch (UnknownHostException e) {

			e.printStackTrace(System.out);

		} catch (IOException e) {

			e.printStackTrace(System.out);

		}

		return false;

	}*/

	

	public static boolean isMYSQLServerRunning(String ip, String path, int listenerPort) {

		Socket requestSocket;

		//String cmdMsg = "\"" + path + "\\mysqladmin\" ping -u root -p111111";

		String cmdMsg = "C:/mysql/bin/mysqladmin ping -u cosar --password=gocosar";

		try {

			requestSocket = new Socket(ip, listenerPort);

			String output = sendMessage(requestSocket, cmdMsg, MYSQL_RUN_CMD);

			if (output.contains("mysqld is alive"))

				return true;

		} catch (UnknownHostException e) {

			e.printStackTrace(System.out);

		} catch (IOException e) {

			e.printStackTrace(System.out);

		}

		return false;

	}



	public static void collectAntiCache(String dist, String hosts,

			String user, String antiCacheFolder) {

		// TODO Auto-generated method stub

		String remoteSSHCmdFormat="";

		if (runningFromLinux){

			remoteSSHCmdFormat= linuxRemoteSSHCmdFormat;

		}

		else{

			remoteSSHCmdFormat=cygwinRemoteSSHCmdFormat;

		}

//		String cmd="C:/cygwin64/bin/ssh "+user+"@ "+ip+" \" killall sar \"";

//		executeRuntime(cmd,false);

		String cmd="";

		String []ips=hosts.split(",");

	//	for(String ip:ips){

		if (!runningFromLinux){

		String pass="Yy567999";

		if (user.equals("mr1")){

			pass="Aa123456";

		}

		cmd="pscp -r -pw "+ pass+"  "+user+"@"+ips[0]+":"+ antiCacheFolder+"/h-store/obj/logs "+ dist;//  ;

		executeRuntime(cmd,false);

		}

		else

		{

			 	cmd=" scp "+sshNoCeck+" -r "+ips[0]+":"+ antiCacheFolder+"/h-store/obj/logs "+ dist;

				executeRuntime(cmd,false);

		}

	//	}

		

		

		

		

		



		

	}

	

	

	public static void copyFilesFromLinux(String dist, String ip,

			String user, String src, boolean removeSrc) {

		// TODO Auto-generated method stub

		String remoteSSHCmdFormat="";

		if (runningFromLinux){

			remoteSSHCmdFormat= linuxRemoteSSHCmdFormat;

		}

		else{

			remoteSSHCmdFormat=cygwinRemoteSSHCmdFormat;

		}

//		String cmd="C:/cygwin64/bin/ssh "+user+"@ "+ip+" \" killall sar \"";

//		executeRuntime(cmd,false);

		String cmd="";

	

		

		if (!runningFromLinux){

		String pass="Yy567999";

		if (user.equals("mr1")){

			pass="Aa123456";

		}

		cmd="pscp -r -pw "+ pass+"  "+ip+":"+src +" "+ dist;//  ;

		executeRuntime(cmd,false);

		}

		else

		{					



			 	cmd="  sudo scp "+sshNoCeck+" -r "+ip+":"+src +" "+ dist ;

				executeRuntime(new String[]{"/bin/sh","-c",cmd},false);

				

				if (removeSrc){

					cmd="  ssh "+sshNoCeck+ ip + " rm -r "+src;

					executeRuntime(new String[]{"/bin/sh","-c",cmd},false);

					

				}

		}

		

		

		

		

		

		



		

	}

	

	

	

	

	

	public static void copyToClients(String src, String bg_destfolder,String [] ips) {





		// TODO Auto-generated method stub

		String localIP="";

		try {

			localIP=InetAddress.getLocalHost().getHostAddress();

		} catch (UnknownHostException e1) {

			// TODO Auto-generated catch block

			e1.printStackTrace(System.out);

		}

		for (int i = 0; i < ips.length; i++) 

		{

			try {



				if (!ips[i].equalsIgnoreCase(localIP)|| src.equals("\\\\127.0.0.1\\BG\\e") )

				{



					long st;

					String destfolder = "\\\\" + ips[i]

							// + "\\c$\\BG";

							+ bg_destfolder;

					System.out.println("Deleting " + destfolder + "...");

					st = System.currentTimeMillis();

					try {

						File dest = new File(destfolder);



						if (!deleteDir(dest))

							System.out.println("Failed to delete " + destfolder);

						System.out.println(" Done! ("

								+ (System.currentTimeMillis() - st) + " msec)");



						st = System.currentTimeMillis();



						File srcFolder = new File(src);



						File destFolder = new File(destfolder);



						System.out.println("Copying " + destfolder + "...");



						copyFolder(srcFolder, destFolder, true,false);



					} catch (IOException e) {

						// TODO Auto-generated catch block

						e.printStackTrace(System.out);

					}

					System.out.println(" Done! ("

							+ (System.currentTimeMillis() - st) + " msec)");

				}

			} catch (Exception e) {

				// TODO Auto-generated catch block

				e.printStackTrace(System.out);

			}}

	}





	public static void copyFromClients(String src, String destfolder, String[] IP, String os) {
		for (int i = 0; i < IP.length; i++) 
		{
			long st;
			String src1 = "\\\\" + IP[i];
			File srcFolder = new File(src1);
			File destFolder = new File( destfolder);

			try {
				if (os.toLowerCase().contains("win")){
					copyFolder(srcFolder, destFolder, true,true);
				}
				else {
					copyFromLinuxRemoteToLocal(IP[i],"",src+"run_output_file.txt",destfolder+"/"+IP[i]+"_run_output_file.txt");
				}
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}
	}

	

	public static void copyFolder(File src, File dest, boolean silent, boolean delSrc)

			throws IOException {

		if (src.isDirectory()) {

			// if directory not exists, create it

			if (!dest.exists()) {

				dest.mkdir();

				if (!silent)

					System.out.println("Directory copied from " + src + "  to "

							+ dest);

			}

			// list all the directory contents

			String files[] = src.list();

			for (String file : files) {

				// construct the src and dest file structure

				File srcFile = new File(src, file);

				File destFile = new File(dest, file);

				// recursive copy

				if (!delSrc||(delSrc&&!srcFile.isDirectory()))

				{

					copyFolder(srcFile, destFile, silent,delSrc);

					if (delSrc)

						deleteDir(srcFile);

				}



			}

		} else {

			// if file, then copy it

			// Use bytes stream to support all file types

			InputStream in = new FileInputStream(src);

			OutputStream out = new FileOutputStream(dest);

			byte[] buffer = new byte[1024];

			int length;

			// copy the file content in bytes

			while ((length = in.read(buffer)) > 0) {

				out.write(buffer, 0, length);

			}



			in.close();

			out.close();

			if (!silent)

				System.out.println("File copied from " + src + " to " + dest);

		}

	}



	

	public static boolean deleteDir(File dir) {

		boolean suc=true;

		if (dir.isDirectory()) {

			String[] children = dir.list();

			for (int i = 0; i < children.length; i++) {

				boolean success = deleteDir(new File(dir, children[i]));

				if (!success) {

					suc=false;

				}

			}

			if (suc==false)

				return false;

		}

		return dir.delete();

	}

}

 class HStoreProcess extends Thread {

	private String cmd = "";

	private String filename = "";

	String name;



	public HStoreProcess (String inputcmd, String processName)

	{

		cmd = inputcmd;

		name=processName;

	}









	public void run()

	{

		String line;

		try {

			Process p = Runtime.getRuntime().exec(cmd);

			BufferedReader input = new BufferedReader (new InputStreamReader(p.getInputStream()));



			FileWriter fout = null;

			if( !filename.equals("") )

			{

				fout = new FileWriter(filename, true);

				fout.write("\r\n\r\n" + new Date().toString() + "\r\n");

			}





			while ( (line = input.readLine()) != null) {

				System.out.println(name+":"+line);

			if (!Utilities.antiCacheInit){

				if (line.contains("H-Store cluster remaining online until killed"))

				{

					Utilities.antiCacheInit=true;

				}

			}

				//				if( fout != null )

				//				{

				//					fout.write(line + "\r\n");

				//				}

			}

			input.close();



			if( fout != null )

			{

				fout.flush();

				fout.close();

			}



			//p.waitFor(); //Causes this thread to wait until the process terminates

		}

		catch (Exception err) {

			err.printStackTrace(System.out);

		}

	}

}









class PrintThread extends Thread{

//private static final String SSHCommandFile = null;
//
//private static final String CommandFile = null;

Process p;

String command;

	public PrintThread(Process p1) {

		// TODO Auto-generated constructor stub

		p=p1;

	}

	public PrintThread(String cmd) {

		// TODO Auto-generated constructor stub

		command=cmd;

	}

	public void run(){

		Process p;

		try {

			//"C:/cygwin64/bin/ssh mr1@10.0.0.150 \"sar -u 10 > cacheosstats/cpu.txt & \""

			p = Runtime.getRuntime().exec(command);

			//_schemaProcess = Runtime.getRuntime().exec("C:/cygwin64/bin/ssh mr1@10.0.0.150 \"killall sar\"");

			

			InputStream stdout = p.getInputStream();

			BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));

			String line = "";

			while ((line = reader.readLine()) != null) {

				System.out.println("Commandout: " + line);

			}

			p.waitFor();

			

			

				

					

				

			

		} catch (IOException | InterruptedException e2) {

			e2.printStackTrace(System.out);

		}

	}
	
	
	
	 
	 
	
	

}




