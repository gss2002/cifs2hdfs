package org.apache.hadoop.cifs;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileOutputStream;

public class HDFS2CifsThread extends Thread {
	SmbFile smbPath;
	HdfsClient hdfsClient;
	String hdfsFile;
	String cifsHost;
	String cifsFolder;
	String cifsSrvPath;
	CifsClient cifsClient;

	public HDFS2CifsThread(CifsClient cifsClient, ThreadGroup tg, String name, String hdfsFile, String cifsHost,
			String cifsFolder, boolean setKrb, String keytabupn, String keytab) {
		super(tg, name);
		this.hdfsFile = hdfsFile;
		this.cifsHost = cifsHost;
		this.cifsFolder = cifsFolder;
		this.cifsSrvPath = "smb://"+cifsHost+"/"+cifsFolder;
		this.cifsClient = cifsClient;
		// Goes to get hadoop configuration files and sets up kerberos and such
		hdfsClient = new HdfsClient(setKrb, keytabupn, keytab);
		

		// Goes to get Hadoop File System Named Node information and such from
		// the configuration.
		hdfsClient.getFS();
	}

	public void run() {
		try {
			uploadCaller();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void uploadCaller() {
		// This says it found a file and to go initiate the file download
		try {
			if (UserGroupInformation.isSecurityEnabled()) {
				hdfsClient.ugi.doAs(new PrivilegedExceptionAction<Void>() {
					public Void run() throws Exception {
						Path hdfsFilePath = new Path(hdfsFile);
						String cifsOutputFile = hdfsFile.split("/")[hdfsFile.split("/").length - 1];
						System.out.println("cifsOutputFile: " + cifsOutputFile);
						SmbFile remoteFile = new SmbFile(cifsSrvPath+"/"+cifsOutputFile, cifsClient.authCtx);
						uploadFile(remoteFile, hdfsFilePath);
						return null;
					}
				});
			} else {
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// This function initiaites the fileupload from HDFS to CIFS 
	public void uploadFile(SmbFile remoteFile, Path hdfsPath) {
		try {
			SmbFileOutputStream out = new SmbFileOutputStream(remoteFile, true);
			BufferedInputStream in = null;
			try {
				byte[] buf = new byte[1048576];
				int bytes_read = 0;

				in = new BufferedInputStream(hdfsClient.fileSystem.open(hdfsPath));

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
