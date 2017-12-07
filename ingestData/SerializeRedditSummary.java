package edu.uchicago.mpcs53013.ingestData;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import edu.uchicago.mpcs53013.redditSummary.RedditSummary;

public class SerializeRedditSummary {
	static TProtocol protocol;
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path("/home/mpcs53013/hadoop/etc/hadoop/core-site.xml"));
			conf.addResource(new Path("/home/mpcs53013/hadoop/etc/hadoop/hdfs-site.xml"));
			final Configuration finalConf = new Configuration(conf);
			final FileSystem fs = FileSystem.get(conf);
			final TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
			RedditSummaryProcessor processor = new RedditSummaryProcessor() {
			Map<Integer, SequenceFile.Writer> partMap = new HashMap<Integer, SequenceFile.Writer>();
			
			Writer getWriter(File file) throws IOException {
				int part = Integer.parseInt(file.getName().substring(11));
				if(!partMap.containsKey(part)) {
					partMap.put(part, 
							SequenceFile.createWriter(finalConf,
									SequenceFile.Writer.file(
											new Path("/inputs/redditData/posts-" + Integer.toString(part))),
									SequenceFile.Writer.keyClass(IntWritable.class),
									SequenceFile.Writer.valueClass(BytesWritable.class),
									SequenceFile.Writer.compression(CompressionType.NONE)));
				}
				return partMap.get(part);
			}

				@Override
				void processRedditSummary(RedditSummary summary, File file) throws IOException {
					try {
						getWriter(file).append(new IntWritable(1), new BytesWritable(ser.serialize(summary)));;
					} catch (TException e) {
						throw new IOException(e);
					}
				}
			};
			processor.processDirectory(args[0]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
