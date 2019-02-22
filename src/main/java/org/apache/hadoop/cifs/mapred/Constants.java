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

public class Constants {
	public static final String CIFS_HOST = "cifs.host";
	public static final String CIFS_USERID = "cifs.userid";
	public static final String CIFS_PASS = "cifs.password";
	public static final String CIFS_DOMAIN = "cifs.domain";
	public static final String CIFS_LOGON_TO = "cifs.logon.to";
	public static final String CIFS_PASS_ALIAS = "cifs.password.alias";

	public static final String CIFS2HDFS_INPUT_FILENAME = "cifs2hdfs.input.filename";
	public static final String CIFS2HDFS_INPUT_FOLDER = "cifs2hdfs.input.folder";
	public static final String CIFS2HDFS_OUTPUT_FOLDER = "cifs2hdfs.output.folder";
	
	public static final String HDFS2CIFS_INPUT_FILENAME = "hdfs2cifs.input.filename";
	public static final String HDFS2CIFS_OUTPUT_FOLDER = "hdfs2cifs.output.folder";
	public static final String HDFS2CIFS_INPUT_FOLDER = "hdfs2cifs.input.folder";


	public static final String NO_NEST = "no.nest";
	public static final String IGNORE_TOP_LEVEL_FOLDER_FILES = "ignore.top.level.folder.files";
	public static final String MAXDEPTH = "maxdepth";
}
