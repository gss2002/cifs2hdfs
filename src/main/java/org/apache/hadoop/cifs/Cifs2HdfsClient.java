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
import org.apache.hadoop.cifs.password.Cifs2HDFSCredentialProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;

public class Cifs2HdfsClient {

	// Static's these won't change they are defined once and only once at start
	// of java program
	static Options options;
	static String hdfsPath;
	static String srv_path;
	static String cifsFolder;
	static String cifsHost;
	static String userId;
	static String pwd;
	static String pwdAlias = null;
	static String pwdCredPath = null;
	static String cifsDomain;
	static String cifsLogonTo;
	static String cifsFile;
	static Integer maxDepth = -1;
	static boolean ignoreTopFolder = false;
	static boolean noNesting = false;
	static boolean cifsTransferLimitTrue = false;
	static String cifsTransferLimit = null;
	private static CifsClient cifsClient;
	private static List<String> cifsFileList;
	private static FileSystem fileSystem;
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
		options.addOption("cifs_folder", true, "CIFS/SMB Server Folder --cifs_folder M201209 ");
		options.addOption("cifs_file", true, "CIFS/SMB Server Single File filename.csv or filename*");
		options.addOption("cifs_userid", true, "CIFS/SMB Domain Userid --cifs_userid usergoeshere");
		options.addOption("cifs_pwd", true, "CIFS/SMB Domain Password --cifs_pwd passwordgoeshere");
		options.addOption("cifs_hadoop_cred_path", true,
				"CIFS Password --cifs_hadoop_cred_path /user/username/credstore.jceks");
		options.addOption("cifs_pwd_alias", true, "CIFS Password Alias --cifs_pwd_alias password.alias");
		options.addOption("cifs_transfer_limit", true,
				"CIFS Client # of transfers to execute simultaneously should not transfer Note: 10-15 = optimal");
		options.addOption("hdfs_outdir", true, "HDFS Output Dir --hdfs_outdir /scm/");
		options.addOption("cifs_max_depth", true, "Max Depth to recurse --cifs_max_depth 10");
		options.addOption("cifs_ignore_top_folder_files", false, "Ignore Top Level Folder files");
		options.addOption("cifs_no_nested_transfer", false, "Do not nest into folders for transfer");
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

		if (cmd.hasOption("cifs_host") && cmd.hasOption("cifs_domain") && cmd.hasOption("cifs_folder")
				&& cmd.hasOption("cifs_userid") && cmd.hasOption("hdfs_outdir")) {
			cifsHost = cmd.getOptionValue("cifs_host");
			cifsDomain = cmd.getOptionValue("cifs_domain");
			cifsFolder = cmd.getOptionValue("cifs_folder");
			userId = cmd.getOptionValue("cifs_userid");
			if (cmd.hasOption("cifs_pwd")) {
				pwd = cmd.getOptionValue("cifs_pwd");
			} else if (cmd.hasOption("cifs_pwd_alias") && cmd.hasOption("cifs_hadoop_cred_path")) {
				pwdAlias = cmd.getOptionValue("cifs_pwd_alias");
				pwdCredPath = cmd.getOptionValue("cifs_hadoop_cred_path");
			} else {
				System.out.println("Missing CIFS Password / CIFS Password Alias / CIFS Hadoop Cred Path");
				missingParams();
				System.exit(0);
			}
			hdfsPath = cmd.getOptionValue("hdfs_outdir");
			if (cmd.hasOption("cifs_ignore_top_folder_files")) {
				ignoreTopFolder = true;
			}
			if (cmd.hasOption("cifs_no_nested_transfer")) {
				noNesting = true;
			}
			if (cmd.hasOption("cifs_logonto")) {
				cifsLogonTo = cmd.getOptionValue("cifs_logonto");
			} else {
				cifsLogonTo = null;
			}
			if (cmd.hasOption("cifs_transfer_limit")) {
				cifsTransferLimitTrue = true;
				cifsTransferLimit = cmd.getOptionValue("cifs_transfer_limit");
			}
			if (cmd.hasOption("cifs_max_depth")) {
				maxDepth = Integer.valueOf(cmd.getOptionValue("cifs_max_depth"));
			}
			if (cmd.hasOption("cifs_file")) {
				cifsFile = cmd.getOptionValue("cifs_file");
				maxDepth = -1;
				noNesting = true;
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

		setConfig();
		UserGroupInformation.setConfiguration(hdpConf);
		if (UserGroupInformation.isSecurityEnabled()) {
			try {
				if (setKrb == true) {
					ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(keytabupn, keytab);
				} else {
					ugi = UserGroupInformation.getCurrentUser();
				}
				System.out.println("UserId for Hadoop: " + ugi.getUserName());
			} catch (IOException e3) {
				// TODO Auto-generated catch block
				e3.printStackTrace();
				System.out.println("Exception Getting Credentials Exiting!");
				System.exit(1);
			}

			System.out.println("HasCredentials: " + ugi.hasKerberosCredentials());
			System.out.println("UserShortName: " + ugi.getShortUserName());
			try {
				System.out.println("Login KeyTab Based: " + UserGroupInformation.isLoginKeytabBased());
				System.out.println("Login Ticket Based: " + UserGroupInformation.isLoginTicketBased());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}

		if (pwdCredPath != null && pwdAlias != null) {
			String pwdCredPathHdfs = pwdCredPath;
			hdpConf.set("hadoop.security.credential.provider.path", pwdCredPathHdfs);
			hdpConf.set(Constants.CIFS2HDFS_PASS_ALIAS, pwdAlias);
			String pwdAlias = hdpConf.get(Constants.CIFS2HDFS_PASS_ALIAS);
			if (pwdAlias != null) {
				char[] pwdChars = null;
				try {
					if (UserGroupInformation.isSecurityEnabled()) {
						System.out.println("SecEnabled: " + true);
						pwdChars = ugi.doAs(new PrivilegedExceptionAction<char[]>() {
							public char[] run() throws Exception {
								Cifs2HDFSCredentialProvider creds = new Cifs2HDFSCredentialProvider();
								char[] pwdChar = creds.getCredentialString(
										hdpConf.get("hadoop.security.credential.provider.path"),
										hdpConf.get(Constants.CIFS2HDFS_PASS_ALIAS), hdpConf);
								return pwdChar;
							}
						});
					} else {
						System.out.println("SecEnabled: " + false);
						Cifs2HDFSCredentialProvider creds = new Cifs2HDFSCredentialProvider();
						pwdChars = creds.getCredentialString(hdpConf.get("hadoop.security.credential.provider.path"),
								conf.get(Constants.CIFS2HDFS_PASS_ALIAS), hdpConf);
					}
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (pwdChars == null) {
					System.out.println("Invalid URI for Password Alias or CredPath");
					System.exit(1);
				} else {
					pwd = new String(pwdChars);
				}
			}

			cifsClient = new CifsClient(cifsLogonTo, userId, pwd, cifsDomain, Integer.valueOf(maxDepth), noNesting);

			SmbFile smbFileConn = cifsClient.createInitialConnection(cifsHost, cifsFolder);

			try {
				cifsClient.traverse(smbFileConn, Integer.valueOf(maxDepth), ignoreTopFolder, cifsFile);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			cifsFileList = cifsClient.getFileList();
			int cifsCount = cifsFileList.size();

			// Spins up a thread per directory to allow some parallelism..
			// Theoretically this can be run as a Mapreduce job
			for (int i = 0; i < cifsCount; i++) {
				String fileName = cifsFileList.get(i);
				Cifs2HDFSThread sc = null;
				try {
					sc = new Cifs2HDFSThread(new SmbFile(fileName, cifsClient.auth), hdfsPath, cifsHost, cifsFolder,
							setKrb, keytabupn, keytab);
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				sc.start();
			}
		}
	}

	private static void missingParams() {
		String header = "CIFS to HDFS Client";
		String footer = "\nPlease report issues at http://github.com/gss2002";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("get", header, options, footer, true);
		System.exit(0);
	}

	// This gets the HDFS File System Config and stores it in a variable for use
	// throughout the program
	public static void getFS() {
		try {
			fileSystem = FileSystem.get(hdpConf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// This gets the Hadoop Configuration files loads them into a Configuration
	// object for use throughout the program
	public static void setConfig() {
		hdpConf = new Configuration();
		hdpConf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		hdpConf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		hdpConf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
		hdpConf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
	}

}
