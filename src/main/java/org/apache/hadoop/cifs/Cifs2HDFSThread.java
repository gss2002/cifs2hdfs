package org.apache.hadoop.cifs;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import jcifs.smb.SmbFile;

public class Cifs2HDFSThread extends Thread {
	SmbFile smbFile;
	HdfsClient hdfsClient;
	String hdfsPath;
	String cifsHost;
	String cifsFolder;

	public Cifs2HDFSThread(ThreadGroup tg, String name, SmbFile smbFile, String hdfsPath, String cifsHost,
			String cifsFolder, boolean setKrb, String keytabupn, String keytab) {
		super(tg, name);
		this.smbFile = smbFile;
		this.hdfsPath = hdfsPath;
		this.cifsHost = cifsHost;
		this.cifsFolder = cifsFolder;
		// Goes to get hadoop configuration files and sets up kerberos and such
		hdfsClient = new HdfsClient(setKrb, keytabupn, keytab);
		

		// Goes to get Hadoop File System Named Node information and such from
		// the configuration.
		hdfsClient.getFS();
	}

	public void run() {
		try {
			downloadCaller();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void downloadCaller() {
		// This says it found a file and to go initiate the file download
		try {
			// Used for Debug commented out
			// System.out.println("SecurityUserNameonThreadFromKerberos:
			// "+ugi.getShortUserName());

			// This block of code in the ugi.doAs is stating execute the
			// downloadFile function with the UserGroupInformation Security
			// handle
			if (UserGroupInformation.isSecurityEnabled()) {

				hdfsClient.ugi.doAs(new PrivilegedExceptionAction<Void>() {
					public Void run() throws Exception {
						if (smbFile.canRead()) {
							System.out.println("SmbFile: " + smbFile.getPath());
							downloadFile(smbFile, hdfsPath);
						} else {
							System.out.println("Unable to read: " + smbFile.getPath());
						}
						return null;
					}
				});
			} else {
				if (smbFile.canRead()) {
					System.out.println("SmbFile: " + smbFile.getPath());
					downloadFile(smbFile, hdfsPath);
				} else {
					System.out.println("Unable to read: " + smbFile.getPath());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// This function initiaites the filedownload from the traverse and passes
	// the data directly into HDFS...
	public void downloadFile(SmbFile remoteFile, String hdfsPath) {
		try {
			// Create a Handle to Java InputStream to prepare to recieve file
			// bytes from SmbFile getInputStream function.
			InputStream in = null;

			// This gets the remoteFile(SmbFile) Handle name from the CIFS/SMb
			// File Share and turns it into a String
			String filename = remoteFile.getName();

			// This gets the remoteFile(SmbFile) Handle Path name from the
			// CIFS/SMb share and removes the CIFS Servername, initialpath
			// and filename as it provides no value to the folder created in
			// HDFS
			String filePath = remoteFile.getPath().replace(this.cifsHost, "").replaceAll(this.cifsFolder, "")
					.replace("smb://", "").replace(" ", "_");
			System.out.println("Transfering File: " + filename + " From " + filePath);

			// This checks and creates a new Path structure w/ the HDFS Path
			// location specified along with the folder name of the file that
			// came from the
			// Cifs server
			Path hdfsWritePath = new Path(hdfsPath + "/" + filePath);
			Path parentPath = hdfsWritePath.getParent();
			if (!(hdfsClient.fileSystem.exists(parentPath))) {
				hdfsClient.fileSystem.mkdirs(parentPath);
			}
			FSDataOutputStream out = hdfsClient.fileSystem.create(hdfsWritePath);
			// This reads the bytes from the SMBFile inputStream below it's
			// buffered with 1MB and uses a BufferedInputStream as CIFS
			// getInputStream
			// is NOT buffered
			try {
				byte[] buf = new byte[1048576];
				int bytes_read = 0;

				in = new BufferedInputStream(remoteFile.getInputStream());

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
			if (out != null) {
				out.close();
			}
			if (in != null) {
				in.close();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
