package org.dicl.velox.mapreduce;

import com.dicl.velox.VeloxDFS;

import java.io.IOException;
import java.lang.Boolean;
import java.lang.InterruptedException;
import java.lang.Math;
import java.lang.System;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

// temp
import java.io.FileOutputStream;

public class LeanRecordReader extends RecordReader<LongWritable, Text> {
	private static final Log LOG = LogFactory.getLog(LeanRecordReader.class);

	// Constants
	//private static final int DEFAULT_BUFFER_SIZE = 2 << 20; // 2 MiB
	private static final int DEFAULT_BUFFER_SIZE = 8388609; // 2 MiB
	private static final int DEFAULT_LINE_BUFFER_SIZE = 8 << 20; // 8 MiB

	// Hadoop stuff
	private LongWritable key = new LongWritable();
	private Text value = new Text();
	private LeanInputSplit split;
	private Counter inputCounter;
	private Counter overheadCounter;
	private Counter readingCounter;
	private Counter startOverheadCounter;
	private Counter nextOverheadCounter;

	private VeloxDFS vdfs = null;
	private long pos = 0;
	private long size = 0;
	private long processedChunks = 0;
	private int bufferOffset = 0;
	private byte[] buffer;

	// Profiling stuff
	private long readingTime = 0;
	private long nextTime = 0;
	private long startConnect = 0;
	private long endConnect = 0;
	private boolean first = true;
	private byte[] lineBuffer;
	private int processed = 0;
	private int start = 0;
	private int curpos = 0;
	private int lentoread = 0;
	private long totalread = 0;
	private int readBytes = 0;
	private int remainingBytes = 0;

	// For Multi Waves
	private int maxProcessBlock = 0;
	private int processBlock = 0;
	private FileOutputStream output;
	private FileOutputStream output2;

private int dReqCount = 0;
private int dNumMaxReq = 10;

	public LeanRecordReader() throws IOException {
	}

	public void initialize(InputSplit split, TaskAttemptContext context) 
		throws IOException, InterruptedException {
			output = new FileOutputStream("/home/velox/java-log.txt");
			output.write("initialize func\n".getBytes());
			startConnect = System.currentTimeMillis();
			this.split = (LeanInputSplit) split;
			size = 0;

			vdfs = new VeloxDFS(this.split.jobID, this.split.taskID, false);
			output2 = new FileOutputStream("/home/velox/spark-logs/lean_record_reader_" + this.split.host + ".log");
			output2.write("initialize func\n".getBytes());

			Configuration conf = context.getConfiguration();

			int bufferSize     = conf.getInt("velox.recordreader.buffersize", DEFAULT_BUFFER_SIZE);
			int lineBufferSize = conf.getInt("velox.recordreader.linebuffersize", DEFAULT_LINE_BUFFER_SIZE);
			
			// For Multi Waves
			maxProcessBlock = conf.getInt("velox.recordreader.maxprocessblock", 16);

			buffer = new byte[bufferSize];
			lineBuffer = new byte[DEFAULT_LINE_BUFFER_SIZE];

			inputCounter = context.getCounter("Lean COUNTERS", LeanInputFormat.Counter.BYTES_READ.name());
			readingCounter = context.getCounter("Lean COUNTERS", "READING_OVERHEAD_MILISECONDS");
			startOverheadCounter = context.getCounter("Lean COUNTERS", "START_OVERHEAD_MILISECONDS");
			nextOverheadCounter = context.getCounter("Lean COUNTERS", "NEXT_OVERHEAD_MILISECONDS");
			endConnect = System.currentTimeMillis();


			LOG.info("Initialized RecordReader for: " + this.split.logicalBlockName	+ " size: " + size 	+ " Host " + this.split.host );
		}

	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean isEOF = false;
		int off = 0;
		final long startTime = System.currentTimeMillis();
		key.set(totalread);

		while(true) {
if (false) {
			output.write(("remainingBytes=" + Integer.toString(remainingBytes)
						+ ", processBlock=" + Integer.toString(processBlock)).getBytes());
}
			if(remainingBytes <= 0) {

if (true) {
				output.write("vdfs.readChunk start\n".getBytes());
				readBytes = vdfs.readChunk(buffer, off);
				output.write("vdfs.readChunk end\n".getBytes());
} else {
				dReqCount++;
				output.write("fake-read start\n".getBytes());
				if (dReqCount < dNumMaxReq) {
					readBytes = 8;
					System.arraycopy("ABCDEFG\n".getBytes(), 0, buffer, 0, 8);
				} else {
					readBytes = 0;
				}
				output.write("fake-read end\n".getBytes());
}

				output.write(("readBytes: "+Integer.toString(readBytes)).getBytes());
				//readBytes = 0;
				if(readBytes <= 0) {
					output.write(("totalread: " + Long.toString(totalread)).getBytes());
					return false;
				}
				remainingBytes = readBytes;
				start = 0;
				curpos = 0;
				
				processBlock++;
			}

			if(curpos == buffer.length - 1 || buffer[curpos] == '\n' || curpos >= readBytes) {
				value.set(buffer, start, curpos - start);
				curpos++;
				start = curpos;
				totalread += curpos - start +  1;
				processed++;
				if(curpos >= readBytes) {
					remainingBytes = 0;
				}
				break;
			} 
			curpos++;
			remainingBytes--;
		}

		final long endTime = System.currentTimeMillis();
		nextTime += (endTime - startTime);
		return true;
	}

	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	public float getProgress() throws IOException, InterruptedException {
		return (float)processed / 40;
	}

	public void close() throws IOException {
		try {
			inputCounter.increment(pos);
			readingCounter.increment(readingTime);
			startOverheadCounter.increment(endConnect - startConnect);
			nextOverheadCounter.increment(nextTime);
		} catch (Exception e) { 
			LOG.error("Fails to close the connection to ZooKeeper");
		}

		
	}
}
