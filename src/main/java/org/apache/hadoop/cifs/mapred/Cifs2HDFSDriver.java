/**
f * Licensed to the Apache Software Foundation (ASF) under one
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
import org.apache.hadoop.cifs.password.CifsCredentialProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Cifs2HDFSDriver {
	static Options options;
	static FileSystem fileSystem;
	static Integer maxDepth = -1;
	static boolean ignoreTopFolder = false;
	static boolean noNesting = false;
	static boolean transferLimitTrue = false;
	static boolean hdfs2cifs = false;
	static boolean cifs2hdfs = false;
	static String transferLimit = null;
	static String cifsUserId;
	static String cifsPwd;
	static String cifsPwdAlias = null;
	static String cifsPwdCredPath = null;
	static String cifsHost;
	static String cifsDomain;
	static String cifsLogonTo;
	static String cifsInputFile;
	static String cifsInputFolder;
	static String cifsOutputFolder;
	static String hdfsInputFile;
	static String hdfsInputFolder;
	static String hdfsOutputFolder;

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
		//options.addOption("hdfs_input_file", true, "HDFS Single Input File filename.csv or filename*");

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
			/*	if (cmd.hasOption("hdfs_input_file")) {
					hdfsInputFile = cmd.getOptionValue("hdfs_input_file");
					maxDepth = -1;
					noNesting = true;
				}
			*/
			}
			if (cmd.hasOption("hdfs_output_folder") && cmd.hasOption("cifs_input_folder")) {
				cifsInputFolder = cmd.getOptionValue("cifs_input_folder");
				if (!(cifsInputFolder.startsWith("/"))) {
					cifsInputFolder = "/"+cifsInputFolder;
				}
				if (!(cifsInputFolder.endsWith("/"))) {
					cifsInputFolder = cifsInputFolder+"/";
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

		if (System.getProperty("oozie.action.conf.xml") != null) {
			conf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));
		}
		conf.set(Constants.CIFS_HOST, cifsHost);
		conf.set(Constants.CIFS_USERID, cifsUserId);
		if (cifsPwd != null) {
			conf.set(Constants.CIFS_PASS, cifsPwd);
		}
		if (cifsDomain != null) {
			conf.set(Constants.CIFS_DOMAIN, cifsDomain);
		}
		if (maxDepth != -1) {
			conf.setInt(Constants.MAXDEPTH, maxDepth);
		}
		conf.setBoolean(Constants.NO_NEST, noNesting);
		conf.setBoolean(Constants.IGNORE_TOP_LEVEL_FOLDER_FILES, ignoreTopFolder);

		if (cifsLogonTo != null) {
			conf.set(Constants.CIFS_LOGON_TO, cifsLogonTo);
		}
		String jobname = null;

		if (cifs2hdfs) {
			if (cifsInputFolder != null) {
				conf.set(Constants.CIFS2HDFS_INPUT_FOLDER, cifsInputFolder);
			}
			if (hdfsOutputFolder != null) {
				conf.set(Constants.CIFS2HDFS_OUTPUT_FOLDER, hdfsOutputFolder);
			}
			if (cifsInputFile != null) {
				conf.set(Constants.CIFS2HDFS_INPUT_FILENAME, cifsInputFile);
			}
			if (cifsInputFolder != null && hdfsOutputFolder != null) {
				jobname = "cifs2hdfs_" + cifsHost + "_" + cifsInputFolder.replace("/", "_") + "_"
						+ hdfsOutputFolder.replace("/", "_");
			} else {
				jobname = "cifs2hdfs_" + cifsHost;
			}
		}
		if (hdfs2cifs) {
			if (hdfsInputFolder != null) {
				conf.set(Constants.HDFS2CIFS_INPUT_FOLDER, hdfsInputFolder);
			}
			if (hdfsInputFolder != null) {
				conf.set(Constants.HDFS2CIFS_INPUT_FOLDER, hdfsInputFolder);
			}
			if (cifsOutputFolder != null) {
				conf.set(Constants.HDFS2CIFS_OUTPUT_FOLDER, cifsOutputFolder);
			}
			if (cifsOutputFolder != null && hdfsInputFolder != null) {
				jobname = "hdfs2cifs_" + cifsHost + "_" + hdfsInputFolder.replace("/", "_") + "_"
						+ cifsOutputFolder.replace("/", "_");
			} else {
				jobname = "hdfs2cifs_" + cifsHost;
			}
		}
		//conf.set("mapreduce.map.java.opts", "-Xmx5120m");
		if (cifsPwdCredPath != null && cifsPwdAlias != null) {
			String pwdCredPathHdfs = cifsPwdCredPath;
			conf.set("hadoop.security.credential.provider.path", pwdCredPathHdfs);
			conf.set(Constants.CIFS_PASS_ALIAS, cifsPwdAlias);
			String pwdAlias = conf.get(Constants.CIFS_PASS_ALIAS);
			if (pwdAlias != null) {
				CifsCredentialProvider creds = new CifsCredentialProvider();
				char[] pwdChars = creds.getCredentialString(conf.get("hadoop.security.credential.provider.path"),
						conf.get(Constants.CIFS_PASS_ALIAS), conf);
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

		if (transferLimitTrue) {
			conf.set("mapreduce.job.running.map.limit", transferLimit);
		}

		try {
			Job job = Job.getInstance(conf, jobname);
			job.addCacheFile(new Path("/apps/cifs2hdfs/jcifs.jar").toUri());
			job.addArchiveToClassPath(new Path("/apps/cifs2hdfs/jcifs.jar"));
			if (cifs2hdfs) {
				job.setJarByClass(Cifs2HDFSDriver.class);
				job.setInputFormatClass(Cifs2HDFSInputFormat.class);
				job.setOutputFormatClass(Cifs2HDFSOutputFormat.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(NullWritable.class);
				job.setMapperClass(Cifs2HDFSCoreMapper.class);
				job.setNumReduceTasks(0);
				FileOutputFormat.setOutputPath(job, new Path(hdfsOutputFolder));
			}
			if (hdfs2cifs) {
				job.setJarByClass(Cifs2HDFSDriver.class);
				job.setInputFormatClass(HDFS2CifsInputFormat.class);
				job.setOutputFormatClass(HDFS2CifsOutputFormat.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(NullWritable.class);
				job.setMapperClass(HDFS2CifsCoreMapper.class);
				job.setNumReduceTasks(0);
				FileInputFormat.addInputPaths(job, hdfsInputFolder);
			}
			job.waitForCompletion(true);
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void missingParams() {
		String header = "Hadoop CIFS Access";
		String footer = "\nPlease report issues at http://github.com/gss2002/cifs2hdfs";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("cifs2hdfs_mr", header, options, footer, true);
		System.exit(0);
	}
}
