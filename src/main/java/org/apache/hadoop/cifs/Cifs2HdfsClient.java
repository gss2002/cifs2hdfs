/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.cifs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import jcifs.smb.SmbFile;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.cifs.mapred.Constants;
import org.apache.hadoop.cifs.password.CifsCredentialProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;

public class Cifs2HdfsClient {

	// Static's these won't change they are defined once and only once at start
	// of java program
	static Object objectWaiter = new Object();
	static Options options;
	static HdfsClient hdfsClient;
	static String hdfsInputFolder;
	static String hdfsOutputFolder;
	static String cifsInputFolder;
	static String cifsOutputFolder;
	static String cifsHost;
	static String cifsUserId;
	static String cifsPwd;
	static String cifsPwdAlias = null;
	static String cifsPwdCredPath = null;
	static String cifsDomain;
	static String cifsLogonTo;
	static String cifsInputFile;
	static Integer maxDepth = -1;
	static boolean ignoreTopFolder = false;
	static boolean noNesting = false;
	static boolean transferLimitTrue = false;
	static boolean hdfs2cifs = false;
	static boolean cifs2hdfs = false;
	static String transferLimit = null;

	private static CifsClient cifsClient;
	private static List<String> cifsFileList;
	// private static FileSystem fileSystem;
	static boolean setKrb = false;
	static String keytab = null;
	static String keytabupn = null;

	// These are used at Thread creation time during the recursion of the files.
	// These define the Smb/CIFS file handle into the thread
	// They also define Hadoop Security Configuration allowing the program to
	// talk directly to HDFS
	static Configuration hdpConf;
	static UserGroupInformation ugi;
	SmbFile f;
	SmbFile inSmbFile;

	// This is the main.. This starts the entire program execution.
	public static void main(String[] args) {

		// This handles parsing args.. This is a really crappy implementation. I
		// have a better one I can share from Commons-cli package

		Configuration conf = new Configuration();
		String[] otherArgs = null;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		} catch (IOException e4) {
			// TODO Auto-generated catch block
			e4.printStackTrace();
		}

		options = new Options();
		options.addOption("cifs_host", true, "CIFS/SMB Server Hostname --cifs_host winfileserver1.nt.example.com");
		options.addOption("cifs_domain", true, "CIFS/SMB Domain --cifs_domain nt.example.com");
		options.addOption("cifs_logonto", true, "CIFS/SMB LogonTo --cifs_logonto windc1nt, hadoopserver");
		options.addOption("cifs_input_folder", true, "CIFS/SMB Server Input Folder --cifs_input_folder M201209 ");
		options.addOption("cifs_output_folder", true, "CIFS/SMB Server Output Folder --cifs_output_folder M201209 ");
		options.addOption("cifs_input_file", true, "CIFS/SMB Server Single Input File filename.csv or filename*");
		options.addOption("cifs_userid", true, "CIFS/SMB Domain Userid --cifs_userid usergoeshere");
		options.addOption("cifs_pwd", true, "CIFS/SMB Domain Password --cifs_pwd passwordgoeshere");
		options.addOption("cifs_hadoop_cred_path", true,
				"CIFS Password --cifs_hadoop_cred_path /user/username/credstore.jceks");
		options.addOption("cifs_pwd_alias", true, "CIFS Password Alias --cifs_pwd_alias password.alias");
		options.addOption("transfer_limit", true,
				"# of transfers to execute simultaneously should not transfer Note: 10-15 = optimal --transfer_limit 10");
		options.addOption("max_depth", true, "CIFS ONLY - Max Depth to recurse --max_depth 10");
		options.addOption("ignore_top_folder_files", false, "CIFS ONLY - Ignore Top Level Folder files");
		options.addOption("no_nested_transfer", false, "CIFS ONLY - Do not nest into folders for transfer");
		options.addOption("hdfs_output_folder", true, "HDFS Output Folder --hdfs_output_dir /scm/");
		options.addOption("hdfs_input_folder", true, "HDFS Input Folder --hdfs_input_dir /scm/");
		// options.addOption("hdfs_input_file", true, "HDFS Single Input File
		// filename.csv or filename*");

		options.addOption("krb_keytab", true, "KeyTab File to Connect to HDFS --krb_keytab $HOME/S00000.keytab");
		options.addOption("krb_upn", true,
				"Kerberos Princpial for Keytab to Connect to HDFS --krb_upn S00000@EXAMP.EXAMPLE.COM");
		options.addOption("help", false, "Display help");

		CommandLineParser parser = new CIFSParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, otherArgs);
		} catch (ParseException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		if (cmd.hasOption("cifs_host") && cmd.hasOption("cifs_domain") && cmd.hasOption("cifs_userid")) {
			cifsHost = cmd.getOptionValue("cifs_host");
			cifsDomain = cmd.getOptionValue("cifs_domain");
			cifsUserId = cmd.getOptionValue("cifs_userid");
			if (cmd.hasOption("cifs_pwd")) {
				cifsPwd = cmd.getOptionValue("cifs_pwd");
			} else if (cmd.hasOption("cifs_pwd_alias") && cmd.hasOption("cifs_hadoop_cred_path")) {
				cifsPwdAlias = cmd.getOptionValue("cifs_pwd_alias");
				cifsPwdCredPath = cmd.getOptionValue("cifs_hadoop_cred_path");
			} else {
				System.out.println("Missing CIFS Password / CIFS Password Alias / CIFS Hadoop Cred Path");
				missingParams();
				System.exit(0);
			}
			if (cmd.hasOption("cifs_logonto")) {
				cifsLogonTo = cmd.getOptionValue("cifs_logonto");

			} else {
				cifsLogonTo = null;
			}
			if (cmd.hasOption("ignore_top_folder_files")) {
				ignoreTopFolder = true;
			}
			if (cmd.hasOption("no_nested_transfer")) {
				noNesting = true;
			}
			if (cmd.hasOption("transfer_limit")) {
				transferLimitTrue = true;
				transferLimit = cmd.getOptionValue("transfer_limit");
			}
			if (cmd.hasOption("max_depth")) {
				maxDepth = Integer.valueOf(cmd.getOptionValue("max_depth"));
			}
			if (cmd.hasOption("hdfs_input_folder") && cmd.hasOption("cifs_output_folder")) {
				hdfsInputFolder = cmd.getOptionValue("hdfs_input_folder");
				cifsOutputFolder = cmd.getOptionValue("cifs_output_folder");
				hdfs2cifs = true;
				if (!(cifsOutputFolder.startsWith("/"))) {
					cifsOutputFolder = "/" + cifsOutputFolder;
					cifsOutputFolder.substring(1, cifsOutputFolder.length());
				}
				if (!(cifsOutputFolder.endsWith("/"))) {
					cifsOutputFolder.substring(0, cifsOutputFolder.length()-1);
				}
				/*
				 * if (cmd.hasOption("hdfs_input_file")) { hdfsInputFile =
				 * cmd.getOptionValue("hdfs_input_file"); maxDepth = -1; noNesting = true; }
				 */
			}
			if (cmd.hasOption("hdfs_output_folder") && cmd.hasOption("cifs_input_folder")) {
				cifsInputFolder = cmd.getOptionValue("cifs_input_folder");
				if (!(cifsInputFolder.startsWith("/"))) {
					cifsInputFolder = "/" + cifsInputFolder;
				}
				if (!(cifsInputFolder.endsWith("/"))) {
					cifsInputFolder = cifsInputFolder + "/";
				}
				hdfsOutputFolder = cmd.getOptionValue("hdfs_output_folder");
				cifs2hdfs = true;
				if (cmd.hasOption("cifs_input_file")) {
					cifsInputFile = cmd.getOptionValue("cifs_input_file");
					maxDepth = -1;
					noNesting = true;
				}
			}
			if (cifs2hdfs && hdfs2cifs) {
				System.out.println(
						"Error Cannot specify hdfs_output_folder/hdfs_input_folder or cifs_output_folder/cifs_input_folder together");
				missingParams();
				System.exit(0);
			}

		} else {
			missingParams();
			System.exit(0);
		}

		if (cmd.hasOption("krb_keytab") && cmd.hasOption("krb_upn")) {
			setKrb = true;
			keytab = cmd.getOptionValue("krb_keytab");
			keytabupn = cmd.getOptionValue("krb_upn");
			File keytabFile = new File(keytab);
			if (keytabFile.exists()) {
				if (!(keytabFile.canRead())) {
					System.out.println("KeyTab  exists but cannot read it - exiting");
					missingParams();
					System.exit(1);
				}
			} else {
				System.out.println("KeyTab doesn't exist  - exiting");
				missingParams();
				System.exit(1);
			}
		}
		hdfsClient = new HdfsClient(setKrb, keytabupn, keytab);
		hdfsClient.checkSecurity();

		if (cifsPwdCredPath != null && cifsPwdAlias != null) {
			cifsPwd = hdfsClient.getCredsViaJceks(cifsPwdCredPath, cifsPwdAlias);
		}

		if (hdfs2cifs) {
			cifsClient = new CifsClient(cifsLogonTo, cifsUserId, cifsPwd, cifsDomain, -1, false);
			List<String> hdfsfileList = null;
			try {
				hdfsfileList = hdfsClient.getHdfsFiles(hdfsInputFolder);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			// Spins up a thread per directory to allow some parallelism..
			// Theoretically this can be run as a Mapreduce job
			ThreadGroup cifsTg = new ThreadGroup("CifsThreadGroup");

			for (int i = 0; i < hdfsfileList.size(); i++) {
				String fileName = hdfsfileList.get(i);
				HDFS2CifsThread sc = null;
				if (transferLimitTrue) {
					while (Integer.valueOf(transferLimit) == cifsTg.activeCount()) {
						synchronized (objectWaiter) {
							try {
								objectWaiter.wait(10000L);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				}
				String threadName = "cifs" + i;
				sc = new HDFS2CifsThread(cifsClient, cifsTg, threadName, fileName, cifsHost, cifsOutputFolder, setKrb,
						keytabupn, keytab);

				sc.start();
			}
		}

		if (cifs2hdfs) {
			cifsClient = new CifsClient(cifsLogonTo, cifsUserId, cifsPwd, cifsDomain, Integer.valueOf(maxDepth),
					noNesting);

			SmbFile smbFileConn = cifsClient.createInitialConnection(cifsHost, cifsInputFolder);

			try {
				cifsClient.traverse(smbFileConn, Integer.valueOf(maxDepth), ignoreTopFolder, cifsInputFile);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			cifsFileList = cifsClient.getFileList();
			int cifsCount = cifsFileList.size();

			// Spins up a thread per directory to allow some parallelism..
			// Theoretically this can be run as a Mapreduce job
			ThreadGroup cifsTg = new ThreadGroup("CifsThreadGroup");

			for (int i = 0; i < cifsCount; i++) {
				String fileName = cifsFileList.get(i);
				Cifs2HDFSThread sc = null;
				if (transferLimitTrue) {
					while (Integer.valueOf(transferLimit) == cifsTg.activeCount()) {
						synchronized (objectWaiter) {
							try {
								objectWaiter.wait(10000L);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				}
				try {
					String threadName = "cifs" + i;
					sc = new Cifs2HDFSThread(cifsTg, threadName, new SmbFile(fileName, cifsClient.authCtx),
							hdfsOutputFolder, cifsHost, cifsInputFolder, setKrb, keytabupn, keytab);
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				sc.start();
			}
		}
	}

	private static void missingParams() {
		String header = "CIFS to Hadoop Client";
		String footer = "\nPlease report issues at http://github.com/gss2002/cifs2hdfs";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("cifs2hdfs", header, options, footer, true);
		System.exit(0);
	}

}
