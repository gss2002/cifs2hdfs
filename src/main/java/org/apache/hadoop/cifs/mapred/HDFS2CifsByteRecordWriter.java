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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.cifs.CifsClient;
import org.apache.hadoop.cifs.password.CifsCredentialProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileOutputStream;

public class HDFS2CifsByteRecordWriter extends RecordWriter<Text, NullWritable> {
	private SmbFileOutputStream out;
	private Configuration conf;
	private FileSystem fs;
	private CifsClient cifsClient;
	private String cifsHost;
	private String cifsFolder;
	private String cifsSrvPath;
	private InputStream in = null;
	private static final Log LOG = LogFactory.getLog(Cifs2HDFSOutputFormat.class.getName());

	public HDFS2CifsByteRecordWriter(Configuration confIn) {
		String pwd = confIn.get(Constants.CIFS_PASS);
		String pwdAlias = confIn.get(Constants.CIFS_PASS_ALIAS);
		if (pwdAlias != null) {
			LOG.info("Cred Provider: " + confIn.get("hadoop.security.credential.provider.path"));
			LOG.info("Cred Alias: " + confIn.get(Constants.CIFS_PASS_ALIAS));

			CifsCredentialProvider creds = new CifsCredentialProvider();
			pwd = new String(creds.getCredentialString(confIn.get("hadoop.security.credential.provider.path"),
					confIn.get(Constants.CIFS_PASS_ALIAS), confIn));
		}
		LOG.info("CIFS_HOST: " + confIn.get(Constants.CIFS_HOST));
		this.cifsClient = new CifsClient(confIn.get(Constants.CIFS_LOGON_TO), confIn.get(Constants.CIFS_USERID), pwd,
				confIn.get(Constants.CIFS_DOMAIN), -1, false);
		this.cifsHost = confIn.get(Constants.CIFS_HOST);
		this.cifsFolder = confIn.get(Constants.HDFS2CIFS_OUTPUT_FOLDER);
		this.cifsSrvPath = "smb://"+cifsHost+"/"+cifsFolder;
		LOG.info("cifsSrvPath: " + cifsSrvPath);
		this.conf = confIn;
	}

	@Override
	public void write(Text key, NullWritable value) throws IOException {
		String hdfsFileString = key.toString();
		Path hdfsFile = new Path(hdfsFileString);
		fs = hdfsFile.getFileSystem(conf);

		String cifsOutputFile = hdfsFileString.split("/")[hdfsFileString.split("/").length - 1];
		LOG.info("cifsOutputFile: " + cifsOutputFile);

		SmbFile remoteFile = new SmbFile(cifsSrvPath+"/"+cifsOutputFile, cifsClient.authCtx);
		out = new SmbFileOutputStream(remoteFile, true);

		key = null;
		value = null;
		LOG.info("CIFS Client Uploading" + cifsOutputFile +" to "+cifsSrvPath);

		// This reads the bytes from the SMBFile inputStream below it's
		// buffered
		// with 1MB and uses a BufferedInputStream as CIFS getInputStream
		// is NOT buffered
		try {
			byte[] buf = new byte[1048576];
			int bytes_read = 0;

			this.in = new BufferedInputStream(fs.open(hdfsFile));


			do {
				bytes_read = in.read(buf, 0, buf.length);

				if (bytes_read < 0) {
					/* Handle EOF however you want */
				}

				if (bytes_read > 0)
					out.write(buf, 0, bytes_read);
				out.flush();

			} while (bytes_read >= 0);

		} catch (IOException e) {
			e.printStackTrace(System.err);
		}
	}

	@Override
	public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		if (out != null) {
			this.out.close();
		}
		if (in != null) {
			this.in.close();
		}
	}
}
