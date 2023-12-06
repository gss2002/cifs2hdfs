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
import java.io.DataOutputStream;
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

public class Cifs2HDFSByteRecordWriter extends RecordWriter<Text, NullWritable> {
	private DataOutputStream out;
	private Configuration conf;
	private FileSystem fs;
	private Path path;
	private CifsClient cifsClient;
	private String cifsHost;
	private String cifsFolder;
	private InputStream in = null;
	private static final Log LOG = LogFactory.getLog(Cifs2HDFSOutputFormat.class.getName());

	public Cifs2HDFSByteRecordWriter(Path path, Configuration confIn) {
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
		this.cifsClient = new CifsClient(confIn.get(Constants.CIFS_LOGON_TO),
				confIn.get(Constants.CIFS_USERID), pwd, confIn.get(Constants.CIFS_DOMAIN), -1, false);
		this.cifsHost = confIn.get(Constants.CIFS_HOST);
		this.cifsFolder = confIn.get(Constants.CIFS2HDFS_INPUT_FOLDER);

		this.conf = confIn;
		this.path = path;
	}

	@Override
	public void write(Text key, NullWritable value) throws IOException {

		String remoteFileIn = key.toString();
		SmbFile remoteFile = new SmbFile(remoteFileIn, cifsClient.authCtx);
		if (remoteFile.canRead()) {
			String parentPath = path.getParent().toString();
			String fileOut = remoteFileIn.replace(cifsHost, "").replace(cifsFolder, "").replace("smb://", "")
					.replace(" ", "_");
			String fileString = parentPath + "/" + fileOut;
			Path file = new Path(fileString);
			Path parPath = file.getParent();
			this.fs = file.getFileSystem(conf);
			if (!(fs.exists(parPath))) {
				fs.mkdirs(parPath);
			}

			this.out = fs.create(file, false);

			key = null;
			value = null;
			LOG.info("CIFS Client Downloading" + remoteFileIn);

			// This reads the bytes from the SMBFile inputStream below it's
			// buffered
			// with 1MB and uses a BufferedInputStream as CIFS getInputStream
			// is NOT buffered
			try {
				byte[] buf = new byte[1048576];
				int bytes_read = 0;

				this.in = new BufferedInputStream(remoteFile.getInputStream());

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
		} else {
			LOG.error("Unable to read: " + remoteFile.getPath().toString());
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
