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

package org.apache.hadoop.cifs.mapred;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.cifs.CIFSParser;
import org.apache.hadoop.cifs.password.Cifs2HDFSCredentialProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Cifs2HDFSDriver {
	static Options options;
	static FileSystem fileSystem;
	static String hdfsPathIn;
	static String srv_path;
	static String folderIn;
	static String cifshostIn;
	static String userIdIn;
	static String pwdIn;
	static String pwdAlias = null;
	static String pwdCredPath = null;
	static String cifsdomainin;
	static String cifsLogonTo;
	static Integer maxDepth = -1;
	static boolean nesTed = false;
	static boolean ignoreTopFolder = false;
	static boolean noNesting = false;
	static boolean cifsTransferLimitTrue = false;
	static String cifsTransferLimit = null;

	public static void main(String[] args) {
		String hdfsIn = null;

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
		options.addOption("cifs_userid", true, "CIFS/SMB Domain Userid --cifs_userid usergoeshere");
		options.addOption("cifs_pwd", true, "CIFS/SMB Domain Password --cifs_pwd passwordgoeshere");
	    options.addOption("cifs_hadoop_cred_path", true, "CIFS Password --cifs_hadoop_cred_path /user/username/credstore.jceks");
	    options.addOption("cifs_pwd_alias", true, "CIFS Password Alias --cifs_pwd_alias password.alias");
		options.addOption("cifs_transfer_limit", true,
				"CIFS Client # of transfers to execute simultaneously should not transfer Note: 10-15 = optimal");
		options.addOption("hdfs_outdir", true, "HDFS Output Dir --hdfs_outdir /scm/");
		options.addOption("cifs_max_depth", true, "Max Depth to recurse --cifs_max_depth 10");
		options.addOption("cifs_ignore_top_folder_files", false, "Ignore Top Level Folder files");
		options.addOption("cifs_no_nested_transfer", false, "Do not nest into folders for transfer");
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
			cifshostIn = cmd.getOptionValue("cifs_host");
			cifsdomainin = cmd.getOptionValue("cifs_domain");
			folderIn = cmd.getOptionValue("cifs_folder");
			userIdIn = cmd.getOptionValue("cifs_userid");
    		if (cmd.hasOption("cifs_pwd")) {
    			pwdIn = cmd.getOptionValue("cifs_pwd");
    		} else if (cmd.hasOption("cifs_pwd_alias") && cmd.hasOption("cifs_hadoop_cred_path")) {
    			pwdAlias = cmd.getOptionValue("cifs_pwd_alias");
    			pwdCredPath = cmd.getOptionValue("cifs_hadoop_cred_path");
    		} else {
    			System.out.println("Missing CIFS Password / CIFS Password Alias / CIFS Hadoop Cred Path");
    			missingParams();
    			System.exit(0);
    		}
			
			hdfsPathIn = cmd.getOptionValue("hdfs_outdir");
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
				maxDepth=Integer.valueOf(cmd.getOptionValue("cifs_max_depth"));
			}

		} else {
			missingParams();
			System.exit(0);
		}

		if (System.getProperty("oozie.action.conf.xml") != null) {
			conf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));
		}
		conf.set(Constants.CIFS2HDFS_HOST, cifshostIn);
		conf.set(Constants.CIFS2HDFS_USERID, userIdIn);
		if (pwdIn != null) {
			conf.set(Constants.CIFS2HDFS_PASS, pwdIn);
		}
		if (cifsdomainin != null) {
			conf.set(Constants.CIFS2HDFS_DOMAIN, cifsdomainin);
		}
		if (folderIn != null) {
			conf.set(Constants.CIFS2HDFS_FOLDER, folderIn);
		}
		
		if (maxDepth != -1) {
			conf.setInt(Constants.CIFS2HDFS_MAXDEPTH, maxDepth);
		}
		conf.setBoolean(Constants.CIFS2HDFS_NO_NEST, noNesting);
		conf.setBoolean(Constants.CIFS2HDFS_IGNORE_TOP_LEVEL_FOLDER_FILES, ignoreTopFolder);

		if (cifsLogonTo != null) {
			conf.set(Constants.CIFS2HDFS_LOGON_TO, cifsLogonTo);
		}
		conf.set("mapreduce.map.java.opts", "-Xmx5120m");
		if (pwdCredPath != null && pwdAlias != null) {
			String pwdCredPathHdfs =pwdCredPath;
			conf.set("hadoop.security.credential.provider.path", pwdCredPathHdfs);
			conf.set(Constants.CIFS2HDFS_PASS_ALIAS, pwdAlias);
			String pwdAlias = conf.get(Constants.CIFS2HDFS_PASS_ALIAS);
			if (pwdAlias != null) {
				Cifs2HDFSCredentialProvider creds = new Cifs2HDFSCredentialProvider();
				char[] pwdChars = creds.getCredentialString(conf.get("hadoop.security.credential.provider.path"), conf.get(Constants.CIFS2HDFS_PASS_ALIAS), conf);
				if (pwdChars == null) {
					System.out.println("Invalid URI for Password Alias or CredPath");
					System.exit(1);
				}
			}
		}

		// propagate delegation related props from launcher job to MR job
		if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
			System.out
					.println("HADOOP_TOKEN_FILE_LOCATION is NOT NULL: " + System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
			conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
		}

		String jobname = null;
		if (folderIn != null) {
			jobname = cifshostIn + "_" + folderIn;
		} else {
			jobname = cifshostIn;
		}
		if (cifsTransferLimitTrue) {
			conf.set("mapreduce.job.running.map.limit", cifsTransferLimit);
		}

		try {
			@SuppressWarnings("deprecation")
			Job job = new Job(conf, "CIFS2HDFS-" + jobname);
			job.addCacheFile(new Path("/apps/cifs2hdfs/jcifs.jar").toUri());
			job.addArchiveToClassPath(new Path("/apps/cifs2hdfs/jcifs.jar"));
			job.setJarByClass(Cifs2HDFSDriver.class);
			job.setInputFormatClass(Cifs2HDFSInputFormat.class);
			job.setOutputFormatClass(Cifs2HDFSOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			job.setMapperClass(Cifs2HDFSCoreMapper.class);
			job.setNumReduceTasks(0);
			FileOutputFormat.setOutputPath(job, new Path(hdfsPathIn));

			job.waitForCompletion(true);
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void missingParams() {
		String header = "CIFS to HDFS Client";
		String footer = "\nPlease report issues at http://github.com/gss2002";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("get", header, options, footer, true);
		System.exit(0);
	}
}
