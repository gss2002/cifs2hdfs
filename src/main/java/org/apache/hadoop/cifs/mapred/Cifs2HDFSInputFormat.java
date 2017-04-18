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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.cifs.CifsClient;
import org.apache.hadoop.cifs.password.Cifs2HDFSCredentialProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import jcifs.smb.SmbFile;

public class Cifs2HDFSInputFormat extends FileInputFormat<Text, NullWritable> {
	private static final Log LOG = LogFactory.getLog(Cifs2HDFSInputFormat.class.getName());
	private static CifsClient cifsClient;
	private static List<String> cifsFileList;

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		String cifsFile = null;
		Configuration conf = job.getConfiguration();
		String pwd = conf.get(Constants.CIFS2HDFS_PASS);
		boolean ignoreTopFolder = conf.getBoolean(Constants.CIFS2HDFS_IGNORE_TOP_LEVEL_FOLDER_FILES, false);
		boolean noNesting = conf.getBoolean(Constants.CIFS2HDFS_NO_NEST, false);
		if (conf.get(Constants.CIFS2HDFS_FILENAME) != null) {
			cifsFile = conf.get(Constants.CIFS2HDFS_FILENAME);
		}


		String pwdAlias = conf.get(Constants.CIFS2HDFS_PASS_ALIAS);
		if (pwdAlias != null) {
			Cifs2HDFSCredentialProvider creds = new Cifs2HDFSCredentialProvider();
			pwd = new String(creds.getCredentialString(conf.get("hadoop.security.credential.provider.path"), conf.get(Constants.CIFS2HDFS_PASS_ALIAS), conf));
		}
		Integer maxDepth = conf.getInt(Constants.CIFS2HDFS_MAXDEPTH, -1);

		cifsClient = new CifsClient(conf.get(Constants.CIFS2HDFS_LOGON_TO), conf.get(Constants.CIFS2HDFS_USERID), pwd,
				conf.get(Constants.CIFS2HDFS_DOMAIN), Integer.valueOf(maxDepth), noNesting);

		SmbFile smbFileConn = cifsClient.createInitialConnection(conf.get(Constants.CIFS2HDFS_HOST),
				conf.get(Constants.CIFS2HDFS_FOLDER));
		cifsClient.traverse(smbFileConn, Integer.valueOf(maxDepth), ignoreTopFolder, cifsFile);
		cifsFileList = cifsClient.getFileList();
		int count = cifsFileList.size();
		LOG.info("CIFS Splits: " + count);

		for (int i = 0; i < cifsFileList.size(); i++) {
			LOG.info("Adding Split: " + i);
			String fileName = cifsFileList.get(i);
			LOG.info("CIFS FileName: " + fileName);
			splits.add((InputSplit) new Cifs2HDFSInputSplit(fileName));
		}
		return splits;

	}

	public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		RecordReader<Text, NullWritable> reader = new Cifs2HDFSFileRecordReader(split, context);
		reader.initialize(split, context);
		return reader;
	}

}
