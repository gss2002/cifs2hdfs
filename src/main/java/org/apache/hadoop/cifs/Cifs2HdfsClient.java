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

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;

import jcifs.smb.DosFileFilter;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;

public class Cifs2HdfsClient extends Thread {

	// Static's these won't change they are defined once and only once at start of java program
	static FileSystem fileSystem;
    static String hdfsPathIn;
    static String srv_path;
	static String folderIn;
	static String cifshostIn;
	static String userIdIn;
	static String pwdIn;
	static String cifsdomainin;
	static String cifsLogonTo;
	static boolean nesTed=false;
	static boolean ignoreTopFolder=false;
	static boolean noNesting=false;

	// These are used at Thread creation time during the recursion of the files. These define the Smb/CIFS file handle into the thread
	// They also define Hadoop Security Configuration allowing the program to talk directly to HDFS
	Configuration conf;
    UserGroupInformation ugi;
    SmbFile f;
    SmbFile inSmbFile;
    int maxDepth;

    
    // This allows a new instance of CifsHdfsClient to allow threads to run. This also passes the OS Kerberos ticket from the user running
    // the job to the specific threads that were created from the main method.
    Cifs2HdfsClient( SmbFile f, int maxDepth) {
        this.f = f;
        this.maxDepth = maxDepth;
        // Goes to get hadoop configuration files
    	setConfig();
	    try {
	    	UserGroupInformation.setConfiguration(conf);
	    	ugi = UserGroupInformation.getCurrentUser();
	    	System.out.println("User for HDFS: "+ugi.getUserName());
	    } catch (IOException e3) {
	    	// TODO Auto-generated catch block
	    	e3.printStackTrace();
	    }
	    ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
	    ugi.setConfiguration(conf);
	    // Goes to get Hadoop File System Named Node information and such from the configuration.
	    getFS();
    }

    void downloadCaller(SmbFile fileIn, boolean nested) {
    	// This says it found a file and to go initiate the file download
    	inSmbFile = fileIn;
    	nesTed=nested;
        try {
        	//Used for Debug commented out
        	//System.out.println("SecurityUserNameonThreadFromKerberos: "+ugi.getShortUserName());
        	
        	// This block of code in the ugi.doAs is stating execute the downloadFile function with the UserGroupInformation Security
        	// handle
            ugi.doAs(new PrivilegedExceptionAction<Void>() {

                public Void run() throws Exception {
                	System.out.println("HDFSPath: "+hdfsPathIn);
                	System.out.println("InFile: "+inSmbFile);

                	downloadFile(inSmbFile, hdfsPathIn, nesTed);
                    return null;
                }
            });
        
         // Catch and and all exceptions.
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }

    // The traverse method is used in the main to get the initial directory listing and then its used
    // in each thread to drill through the directories to get to the files
    // This is also where the execution of the HDFS file load occurs
    void traverse( SmbFile f, int depth) throws MalformedURLException, IOException {
    	boolean nested = false;
        if( depth == 0 ) {
            return;
        }

        // This gets the file handle and gets the initial file list.
        if (f.isDirectory()) {
        	SmbFile[] l = f.listFiles();
        	nested=true;
       
        	for(int i = 0; l != null && i < l.length; i++ ) {
        		try {
        			// We don't care about trying to download a directory as it has no representation in Smb/CIFS land
        			// We just continue to traverse the structure
        			if( l[i].isDirectory() ) {
        				traverse( l[i], depth-1 );
                 
        			} else {
        				// This says it found a file and to go initiate the file download
        				inSmbFile = l[i];
        				downloadCaller(inSmbFile, nested);
                    
        			}
                
        		} catch( IOException ioe ) {
        			System.out.println( l[i] + ":" );
        			ioe.printStackTrace( System.out );
        		}
        	}
        } else {
			downloadCaller(f, nested);

        }
    }
    // Seeing that the class extends run this says execute the folders found in the main on a seperate thread.
    public void run() {
        try {
            traverse( f, maxDepth );
        } catch( Exception ex ) {
            ex.printStackTrace();
        }
    }

    // This function initiaites the filedownload from the traverse and passes the data directly into HDFS...
    public void downloadFile(SmbFile remoteFile, String hdfsPath, boolean nested) {
		try {
			// Create a Handle to Java InputStream to prepare to recieve file bytes from SmbFile getInputStream function.
			InputStream in = null;

			// Create a Handle on the root HDFS Path location if it doesn't exist exit..
            Path rootpath = new Path(hdfsPath);
            if (!(fileSystem.exists(rootpath))) {
                    System.out.println("Upload Path does not exist: Exiting");          
                    System.exit(1);
            }
            // This gets the remoteFile(SmbFile) Handle name from the CIFS/SMb File Share and turns it into a String
            String filename = remoteFile.getName();
            
            // This gets the remoteFile(SmbFile) Handle Path name from the CIFS/SMb share and removes the CIFS Servername, initialpath
            // and filename as it provides no value to the folder created in HDFS
            String filePath = remoteFile.getPath().replace(cifshostIn, "").replace(folderIn, "").replace("smb://", "").replace(filename, "");
            System.out.println("Transfering File: "+filename+" From "+filePath);

            // This checks and creates a new Path structure w/ the HDFS Path location specified along with the folder name of the file that came from the
            // Cifs server
            Path hdfsWritePath;

            if (nested) {
            	if (!(fileSystem.exists(new Path(filePath)))) {
            		fileSystem.mkdirs(new Path(filePath));
            	}
            
            	// This creates the file handle into HDFS based on the hdfsRootPath/CIFS Server File path and file name. This then
            	//opens up a file handle as a FSDataoutputstream directly into HDFS 
                System.out.println("Transfering File: "+filename+" From "+remoteFile.getPath().replace(cifshostIn, "").replace("smb://", "").replace(filename, ""));
            	 hdfsWritePath = new Path(hdfsPath+"/"+filePath+"/"+filename);
            } else {
                System.out.println("Transfering File: "+filename+" From "+remoteFile.getPath().replace(cifshostIn, "").replace("smb://", "").replace(filename, ""));
            	 hdfsWritePath = new Path(hdfsPath+"/"+filename);

            }
			FSDataOutputStream out = fileSystem.create(hdfsWritePath);
            
            // This reads the bytes from the SMBFile inputStream below it's buffered with 1MB and uses a BufferedInputStream as CIFS getInputStream
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
	
	//This gets the HDFS File System Config and stores it in a variable for use throughout the program
	  public void getFS() {
	        try {
				fileSystem = FileSystem.get(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	    }
	 
	  // This gets the Hadoop Configuration files loads them into a Configuration object for use throughout the program
	    public void setConfig(){
	    	conf = new Configuration();
	    	conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
	    	conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
	    	conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
	    	conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));

	    }
	 
	    // This is the main.. This starts the entire program execution.
	    public static void main(String[] args) {
	    	String hdfsIn = null;
	    	
	    	// This handles parsing args.. This is a really crappy implementation. I have a better one I can share from Commons-cli package
	    	
	    	Configuration conf = new Configuration();
		    String[] otherArgs = null;
			try {
				otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			} catch (IOException e4) {
				// TODO Auto-generated catch block
				e4.printStackTrace();
			}
			
		    Options options = new Options();
		    options.addOption("cifshost", true, "CIFS/SMB Server Hostname --cifshost winfileserver1.nt.example.com");
		    options.addOption("cifsdomain", true, "CIFS/SMB Domain --cifsdomain nt.example.com");
		    options.addOption("cifslogonto", true, "CIFS/SMB LogonTo --cifslogonto windc1nt, hadoopserver");
		    options.addOption("cifsfolder", true, "CIFS/SMB Server Folder --cifsfolder M201209 ");
		    options.addOption("userid", true, "CIFS/SMB Domain Userid --userid usergoeshere");
		    options.addOption("pwd", true, "CIFS/SMB Domain Password --pwd passwordgoeshere");
		    options.addOption("hdfs_outdir", true, "HDFS Output Dir --hdfs_outdir /scm/");
		    options.addOption("ignore_top_folder_files" , false, "Ignore Top Level Folder files");
		    options.addOption("no_nested_transfer", false , "Do not nest into folders for transfer");
		    options.addOption("help", false, "Display help");
		    CommandLineParser parser = new CIFSParser();
		    CommandLine cmd = null;
			try {
				cmd = parser.parse( options, otherArgs);
			} catch (ParseException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
	    	
		    if (cmd.hasOption("cifshost") && cmd.hasOption("cifsdomain") && cmd.hasOption("cifsfolder") && cmd.hasOption("userid") && cmd.hasOption("pwd") &&
		    		cmd.hasOption("hdfs_outdir")) {
		    	cifshostIn = cmd.getOptionValue("cifshost");
		    	cifsdomainin = cmd.getOptionValue("cifsdomain");
		    	folderIn = cmd.getOptionValue("cifsfolder");
		    	userIdIn = cmd.getOptionValue("userid");
		    	pwdIn = cmd.getOptionValue("pwd");
		    	hdfsPathIn = cmd.getOptionValue("hdfs_outdir");
		    	if (cmd.hasOption("ignore_top_folder_files")) {
		    		ignoreTopFolder = true;
		    	}
		    	if (cmd.hasOption("no_nested_transfer")) {
		    		noNesting=true;
		    	}
		    	if (cmd.hasOption("cifslogonto")) {
		    		cifsLogonTo = cmd.getOptionValue("cifslogonto");

		    	} else {
		    		cifsLogonTo = null;
		    	}
		    	
		    } else {
		    	 String header = "Do something useful with an input file\n\n";
		    	 String footer = "\nPlease report issues at http://example.com/issues";
		    	 HelpFormatter formatter = new HelpFormatter();
		    	 formatter.printHelp("get", header, options, footer, true);
		    	 System.exit(0);
		    }
	    	
			
			
			InetAddress inetAddress = null;
			try {
				inetAddress = InetAddress.getByName(cifsdomainin);
			} catch (UnknownHostException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

    	    // Sets ups JCIFS which is acutally the library used to make the CIFS/SMB Call via Java
	    	jcifs.Config.setProperty("jcifs.smb.client.listSize", "1200");
	    	jcifs.Config.setProperty("jcifs.smb.client.attrExpirationPeriod", "0");
	        jcifs.Config.setProperty("jcifs.netbios.wins", inetAddress.getHostAddress().toString() );
	        // This spoofs logon rights
	        if (cifsLogonTo != null) {
	        	jcifs.Config.setProperty("jcifs.netbios.hostname", cifsLogonTo.toUpperCase());

	        } else {
	        	jcifs.Config.setProperty("jcifs.netbios.hostname", "W"+userIdIn.toUpperCase());
	        }
			NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(cifsdomainin, userIdIn, pwdIn);
			SmbFile f = null;
			// Concats the cifshostin with the initial folder name
			srv_path = cifshostIn+folderIn;
		    try {
				f = new SmbFile("smb://"+srv_path, auth);
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
		    // Creates a filter on * with attributes of directory only
		    DosFileFilter everything = new DosFileFilter( "*", SmbFile.ATTR_DIRECTORY );


	        int maxDepth = Integer.parseInt("7");


	        SmbFile[] l = null;
			try {
				if (ignoreTopFolder) {
					l = f.listFiles(everything);
				} else {
					l = f.listFiles();
				}

			} catch (SmbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// Spins up a thread per directory to allow some parallelism.. Theoretically this can be run as a Mapreduce job
	        for (int i = 0; i < l.length; i++) {
	        	if (noNesting) {
	        		try {
						if (!(l[i].isDirectory())) {
				            Cifs2HdfsClient sc = new Cifs2HdfsClient( l[i], maxDepth );
				            sc.start();	
						}
					} catch (SmbException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	        	} else {
		            Cifs2HdfsClient sc = new Cifs2HdfsClient( l[i], maxDepth );
		            sc.start();
	        	}


	        }
			
	    }
	
	
	
}

