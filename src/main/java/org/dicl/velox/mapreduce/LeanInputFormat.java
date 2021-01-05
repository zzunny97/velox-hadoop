package org.dicl.velox.mapreduce;

import com.dicl.velox.VeloxDFS;
import com.dicl.velox.model.BlockMetadata;
import com.dicl.velox.model.Metadata;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.net.*;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class LeanInputFormat extends InputFormat<LongWritable, Text> {
	private static final Log LOG = LogFactory.getLog(LeanInputFormat.class);
//	private static final long MinSplitSize = 134217728l; // 128MB

	private FileOutputStream fostream;
	//private Writer out;

	public LeanInputFormat() throws IOException {
		fostream = new FileOutputStream("/home/velox/lean_input_format.log");
		//out = new BufferedWriter(new OutputStreamWriter(fostream));
	}

	public static enum Counter {
		BYTES_READ
	}

	public List<InputSplit> getSplits(JobContext job) throws IOException {
		LOG.info("sibal~~~");
		int task_id = job.getJobID().hashCode();
		if(task_id < 0){
			task_id *= -1;
		}
		task_id %= 1000;

		VeloxDFS vdfs = new VeloxDFS(job.getJobID().toString(), task_id, true);

		// Setup Zookeeper ZNODES
		String zkAddress   = job.getConfiguration().get("velox.recordreader.zk-addr", "115.145.173.24:2381");

		LOG.info("zkAddress: " + zkAddress + " " + job.getJobID().toString());
		LeanSession session = new LeanSession(zkAddress, job.getJobID().toString(), 500000);
		session.setupZk();
		session.close();

		// Generate Logical Block distribution
		Configuration conf = job.getConfiguration();
//		String filePath = conf.get("velox.inputfile", "50G.dat");
		//String filePath = "lorem.txt";
		//String filePath = "input1G.dat";
		//String filePath = "input1G_notrailing.dat";
		String filePath = "50G.dat";
		//String filePath = "small_100k.dat";
		//String filePath = "small_200k.dat";
		//String filePath = "small_150k.dat";
		String jobID = job.getJobID().toString();


		long fd = vdfs.open(filePath);
		Metadata md = vdfs.getMetadata(fd, (byte)3);
		//long slotNum = Integer.parseInt(job.getConfiguration().get("mapreduce.task.slot"));
		long slotNum = Integer.parseInt(job.getConfiguration().get("mapreduce.task.slot", "16"));
		//long slotNum = Integer.parseInt(job.getConfiguration().get("mapreduce.task.slot", "2"));
		long MinSplitSize = Integer.parseInt(job.getConfiguration().get("velox.recordreader.buffersize", "8388608")); 
		//long slotNum = 16; 
		//long MinSplitSize = 8388608; 
		// Set important variables 
		// Generate the splits per each generated logical block
		List<InputSplit> splits = new ArrayList<InputSplit>();

		//fostream.write(("numBlock=" + Integer.toString(md.numBlock)).getBytes());
		for (int i = 0; i < md.numBlock; i++) { // md.numBlock => md.numSlot
		//	long sNum = md.blocks[i].size / MinSplitSize + 1;  
		//	sNum = sNum > slotNum ? slotNum : sNum;
			for(long j = 0; j < slotNum; j++){
				LeanInputSplit split = new LeanInputSplit(md.blocks[i].name, md.blocks[i].host, jobID, task_id);
				splits.add(split);
			}
		}

		LOG.info("Total number of chunks " + md.numChunks);
		vdfs.close(fd);
		return splits;
	}

	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, 
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new LeanRecordReader();
	}
}
