package org.apache.hadoop.cifs.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.cifs.CifsClient;
import org.apache.hadoop.cifs.password.CifsCredentialProvider;
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

public class HDFS2CifsInputFormat extends FileInputFormat<Text, NullWritable> {
	private static final Log LOG = LogFactory.getLog(HDFS2CifsInputFormat.class.getName());

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}


	public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		RecordReader<Text, NullWritable> reader = new HDFS2CifsFileRecordReader(split, context);
		reader.initialize(split, context);
		return reader;
	}

}
