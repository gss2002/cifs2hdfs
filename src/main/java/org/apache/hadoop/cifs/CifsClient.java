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

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import jcifs.smb.DosFileFilter;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;

public class CifsClient {
	InetAddress inetAddress = null;
	String cifsFolder;
	String cifshost;
	String userId;
	String pwd;
	String cifsdomain;
	String cifsLogonTo;
	boolean ignoreTopFolder = false;
	boolean noNesting = false;
	int maxDepth;
	DosFileFilter dirOnly;
	SmbFile smbFile = null;
	public NtlmPasswordAuthentication auth;
	private List<String> smbFileList;

	/**
	 * Creates a new <code>CifsClient</code> contains the necessary information
	 * to connect to Cifs/SMB Server.
	 *
	 * @param cifsLogonTo
	 *            Cifs/Smb Logon Server.
	 * @param userId
	 *            the system userid.
	 * @param pwd
	 *            the system password.
	 * @param cifsDomain
	 *            the Cifs/Smb Logon Domain.
	 * @param maxDepth
	 *            the maxDepth to nest into folders.
	 * @param noNesting
	 *            do not go beyond current folder.
	 * @exception IndexOutOfBoundsException
	 *                if the <code>offset</code> and <code>count</code>
	 *                arguments index characters outside the bounds of the
	 *                <code>value</code> array.
	 */

	public CifsClient(String cifsLogonTo, String userId, String pwd, String cifsDomain, Integer maxDepth,
			boolean noNesting) {
		this.pwd = pwd;
		this.cifsLogonTo = cifsLogonTo;
		this.userId = userId;
		this.cifsdomain = cifsDomain;
		this.noNesting = noNesting;

		try {
			inetAddress = InetAddress.getByName(cifsdomain);
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// Sets ups JCIFS which is acutally the library used to make the
		// CIFS/SMB Call via Java
		jcifs.Config.setProperty("jcifs.smb.client.listSize", "1200");
		jcifs.Config.setProperty("jcifs.smb.client.attrExpirationPeriod", "0");
		jcifs.Config.setProperty("jcifs.netbios.wins", inetAddress.getHostAddress().toString());
		// This spoofs logon rights
		if (cifsLogonTo != null) {
			jcifs.Config.setProperty("jcifs.netbios.hostname", cifsLogonTo.toUpperCase());

		} else {
			jcifs.Config.setProperty("jcifs.netbios.hostname", "W" + userId.toUpperCase());
		}
		auth = new NtlmPasswordAuthentication(cifsDomain, userId, pwd);

		// Creates a filter on * with attributes of directory only
		dirOnly = new DosFileFilter("*", SmbFile.ATTR_DIRECTORY);

		this.maxDepth = maxDepth;
	}

	public SmbFile createInitialConnection(String cifsHost, String cifsFolder) {
		// Concats the cifshostin with the initial folder name
		String srv_path = cifsHost + cifsFolder;
		try {
			smbFile = new SmbFile("smb://" + srv_path, this.auth);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return smbFile;
	}

	// The traverse method is used in the main to get the initial directory
	// listing and then its used
	// in each thread to drill through the directories to get to the files
	// This is also where the execution of the HDFS file load occurs
	public void traverse(SmbFile f, int depth, boolean ignoreTopFolder, String cifsFile)
			throws MalformedURLException, IOException {
		boolean fileOnly = false;
		CifsFileFilter fileFilter = null;

		if (smbFileList == null) {
			smbFileList = new ArrayList<String>();
		}
		if (cifsFile != null) {
			fileOnly = true;
			fileFilter = new CifsFileFilter(cifsFile);
		}
		if (depth == 0) {
			return;
		}

		// This gets the file handle and gets the initial file list.
		if (f.isDirectory() && !(noNesting)) {
			SmbFile[] l;
			if (ignoreTopFolder) {
				l = f.listFiles(dirOnly);
			} else if (fileOnly) {
				l = f.listFiles(fileFilter);
			} else {
				l = f.listFiles();
			}

			for (int i = 0; l != null && i < l.length; i++) {
				try {
					// We don't care about trying to download a directory as it
					// has no representation in Smb/CIFS land
					// We just continue to traverse the structure
					if (l[i].isDirectory() || depth != -1) {
						traverse(l[i], depth - 1, false, null);
					} else {
						// This says it found a file and to go initiate the file
						// download
						if (!(ignoreTopFolder)) {
							smbFileList.add(l[i].getURL().toString());
						}
					}
				} catch (IOException ioe) {
					System.out.println(l[i] + ":");
					ioe.printStackTrace(System.out);
				}
			}
		} else {
			SmbFile[] l;
			if (fileOnly) {
				l = f.listFiles(fileFilter);
				for (int i = 0; l != null && i < l.length; i++) {
					if (!(ignoreTopFolder) && !(l[i].isDirectory())) {
						smbFileList.add(l[i].getURL().toString());
					}
				}
			} else {
				if (!(ignoreTopFolder)) {
					smbFileList.add(f.getURL().toString());
				}
			}
		}
	}

	public List<String> getFileList() {
		return this.smbFileList;
	}

}
