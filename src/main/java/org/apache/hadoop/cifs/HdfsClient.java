package org.apache.hadoop.cifs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.cifs.mapred.Constants;
import org.apache.hadoop.cifs.password.CifsCredentialProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class HdfsClient {
	public Configuration conf;
	public UserGroupInformation ugi;
	public FileSystem fileSystem;
	FileStatus[] hdfsFilesList;
	List<String> fileList;


	public HdfsClient(boolean setKrb, String keytabupn, String keytab) {
		setConfig();
		UserGroupInformation.setConfiguration(conf);
		if (UserGroupInformation.isSecurityEnabled()) {
			try {
				if (setKrb == true) {
					ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(keytabupn, keytab);
					UserGroupInformation.setLoginUser(ugi);
				} else {
					UserGroupInformation.setLoginUser(ugi);
					ugi = UserGroupInformation.getCurrentUser();
				}
			} catch (IOException e3) {
				// TODO Auto-generated catch block
				e3.printStackTrace();
				System.out.println("Exception Getting Credentials Exiting!");
				System.exit(1);
			}
		}
	}

	public void checkSecurity() {
		if (UserGroupInformation.isSecurityEnabled()) {
			System.out.println("HasCredentials: " + ugi.hasKerberosCredentials());
			System.out.println("UserShortName: " + ugi.getShortUserName());
			try {
				System.out.println("Login KeyTab Based: " + UserGroupInformation.isLoginKeytabBased());
				System.out.println("Login Ticket Based: " + UserGroupInformation.isLoginTicketBased());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} else {
			System.out.println("Security Not Enabled");
		}
	}

	public String getCredsViaJceks(String cifsPwdCredPath, String cifsPwdAlias) {
		String pwdCredPathHdfs = cifsPwdCredPath;
		conf.set("hadoop.security.credential.provider.path", pwdCredPathHdfs);
		conf.set(Constants.CIFS_PASS_ALIAS, cifsPwdAlias);
		String pwdAlias = conf.get(Constants.CIFS_PASS_ALIAS);
		if (pwdAlias != null) {
			char[] pwdChars = null;
			try {
				if (UserGroupInformation.isSecurityEnabled()) {
					System.out.println("SecEnabled: " + true);
					pwdChars = ugi.doAs(new PrivilegedExceptionAction<char[]>() {
						public char[] run() throws Exception {
							CifsCredentialProvider creds = new CifsCredentialProvider();
							char[] pwdChar = creds.getCredentialString(
									conf.get("hadoop.security.credential.provider.path"),
									conf.get(Constants.CIFS_PASS_ALIAS), conf);
							return pwdChar;
						}
					});
				} else {
					System.out.println("SecEnabled: " + false);
					CifsCredentialProvider creds = new CifsCredentialProvider();
					pwdChars = creds.getCredentialString(conf.get("hadoop.security.credential.provider.path"),
							conf.get(Constants.CIFS_PASS_ALIAS), conf);
				}
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (pwdChars == null) {
				System.out.println("Invalid URI for Password Alias or CredPath");
				System.exit(1);
			} else {
				return new String(pwdChars);
			}
		}
		return "";
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

	public List<String> getHdfsFiles(String hdfsPath) throws IOException, InterruptedException {
		getFS();
		fileList = new ArrayList<String>();
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			public Void run() throws Exception {
				try {
			/*		if (!(hdfsPath.startsWith("/"))) {
						String hdfsPathOut = hdfsPath.replace("./", "");
						if (fileSystem == null) {
							System.out.println("FS = NULL");
						}
						System.out.println("WorkingDir: " + fileSystem.getWorkingDirectory());
						hdfsPathOut = fileSystem.getWorkingDirectory().toUri().getPath() + "/" + hdfsPathOut;
						System.out.println("HdfsDirectory: " + hdfsPath);
					}
					*/
					hdfsFilesList = fileSystem.listStatus(new Path(hdfsPath));
					System.out.println("hdfsFileList.count: "+hdfsFilesList.length);
					for (int i = 0; hdfsFilesList != null && i < hdfsFilesList.length; i++) {
						if (!(hdfsFilesList[i].isDirectory())) {
							fileList.add(hdfsFilesList[i].getPath().toUri().getPath());
						}
					}
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalArgumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
			}
		});

		return fileList;
	}
}
