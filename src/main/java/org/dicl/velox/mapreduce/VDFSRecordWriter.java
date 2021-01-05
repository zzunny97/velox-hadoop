package org.dicl.velox.mapreduce;

import com.dicl.velox.VeloxDFS;

import java.io.IOException;
import java.lang.InterruptedException;
import java.lang.System;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class VDFSRecordWriter extends RecordWriter {
	
	private static final Log LOG = LogFactory.getLog(VDFSRecordWriter.class);
	String outputFile;
	String output = "";
	String forCommit = "";
	VeloxDFS vdfs = null;

	VDFSRecordWriter(TaskAttemptContext context) {
		vdfs = new VeloxDFS("OutputWriter", -1, true);
		
		Configuration config = context.getConfiguration();
		outputFile = config.get("velox.outputfile");
		LOG.info("VDFS Record Writer is Created : " + outputFile);
	}

	@Override
	public void write(Object key, Object value) throws IOException, InterruptedException {
		//output += key.toString() + " " + value.toString();
		output = key.toString() + " " + value.toString();

		// LOG.info("Record: " + key.toString() + " " + value.toString());
		
		//LOG.info("GREP Output : " + output);
		try {
			boolean ret = vdfs.write(outputFile, output.getBytes(), output.getBytes().length, false);
		} catch(Exception e) {
			System.out.println(e);
		}
		
	//	vdfs.write_commit(outputFile);
	//	LOG.info("End Output to VDFS : " + output);
	}

	public void close(TaskAttemptContext context) throws IOException {
		
		System.out.println("Close function");
		boolean ret = vdfs.write(outputFile, forCommit.getBytes(), forCommit.getBytes().length, true);
	//	vdfs.write_commit(outputFile);
		LOG.info(outputFile + " is commited and VDFSRecordWriter is closed");
		
	}
}
