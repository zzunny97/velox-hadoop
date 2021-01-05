package org.dicl.velox.mapreduce;

import com.dicl.velox.VeloxDFS;

import java.io.IOException;
import java.lang.Boolean;
import java.lang.InterruptedException;
import java.lang.Math;
import java.lang.System;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class VDFSOutputFormat extends OutputFormat {
	public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new VDFSRecordWriter(context);
    }

  
    public void checkOutputSpecs(JobContext jc) throws IOException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext tac) throws IOException, InterruptedException {

		return new VDFSOutputCommitter();
	}

}
