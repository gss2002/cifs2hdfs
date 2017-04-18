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
	Configuration conf;
	UserGroupInformation ugi;
	SmbFile smbFile;
	String hdfsPath;
	String cifsHost;
	String cifsFolder;
	FileSystem fileSystem;

	public Cifs2HDFSThread(SmbFile smbFile, String hdfsPath, String cifsHost, String cifsFolder, boolean setKrb,
			String keytabupn, String keytab) {
		this.smbFile = smbFile;
		this.hdfsPath = hdfsPath;
		this.cifsHost = cifsHost;
		this.cifsFolder = cifsFolder;

		// Goes to get hadoop configuration files
		setConfig();

		setConfig();
		UserGroupInformation.setConfiguration(conf);
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

		// Goes to get Hadoop File System Named Node information and such from
		// the configuration.
		getFS();
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

				ugi.doAs(new PrivilegedExceptionAction<Void>() {

					public Void run() throws Exception {
						System.out.println("HDFSPath: " + hdfsPath);
						System.out.println("SmbFile: " + smbFile.getPath());
						downloadFile(smbFile, hdfsPath);
						return null;
					}
				});
			} else {
				System.out.println("HDFSPath: " + hdfsPath);
				System.out.println("SmbFile: " + smbFile.getPath());
				downloadFile(smbFile, hdfsPath);
			}

			// Catch and and all exceptions.
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
			String filePath = remoteFile.getPath().replace(this.cifsHost, "").replace("smb://", "").replace(" ", "_");
			System.out.println("Transfering File: " + filename + " From " + filePath);

			// This checks and creates a new Path structure w/ the HDFS Path
			// location specified along with the folder name of the file that
			// came from the
			// Cifs server
			Path hdfsWritePath = new Path(hdfsPath + "/" + filePath);

			FSDataOutputStream out = fileSystem.create(hdfsWritePath);

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
			out.close();
			in.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// This gets the HDFS File System Config and stores it in a variable for use
	// throughout the program
	public void getFS() {
		try {
			fileSystem = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// This gets the Hadoop Configuration files loads them into a Configuration
	// object for use throughout the program
	public void setConfig() {
		conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));

	}
}
