package com.shan.idstut;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class PageRankPrepJobs {

	int n;
	static Configuration conf;

	public static class UniqNodeCountMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] vals = value.toString().split("\t");
			for(String val:vals)
				context.write(new IntWritable(1), new Text(val));
		}
	}

	public static class UniqNodeCountReducer extends
			Reducer<IntWritable, Text, NullWritable, IntWritable> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Set<String> uniqueNodesSet = new HashSet<String>();
			for (Text val : values) {
				uniqueNodesSet.add(val.toString());
			}
			context.write(null, new IntWritable(uniqueNodesSet.size()));
		}
	}

	public PageRankPrepJobs(String working_fs, String input_path, String output_path) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		conf = new Configuration();
		
		FileSystem fs;
		try {
			fs = FileSystem.get(new URI(working_fs), conf);
		} catch (URISyntaxException exp) {
			fs = FileSystem.get(conf);
			exp.printStackTrace();
		}
		
		if(fs.exists(new Path(output_path))){
			fs.delete(new Path(output_path), true);
		}
		
		Job job = Job.getInstance(conf, "count_uniq_nodes");

		job.setJarByClass(PageRankPrepJobs.class);
		job.setMapperClass(UniqNodeCountMapper.class);
		job.setReducerClass(UniqNodeCountReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		

		FileInputFormat.addInputPath(job, new Path(input_path+"/part*"));
		FileOutputFormat.setOutputPath(job, new Path(output_path+"/temp/num_nodes"));
		
		job.waitForCompletion(true);

		System.out.println("jobs finished");
		
		getMergeInHdfs(fs, output_path+"/temp/num_nodes/", output_path+"/num_nodes");

		populateNumOfNodes(fs, output_path);
		
		fs.close();
	}
	
	private void populateNumOfNodes(FileSystem fs, String output_path) throws IllegalArgumentException, IOException {
		FSDataInputStream in = null;
		try {
			in = fs.open(new Path(output_path+"/num_nodes"));
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			IOUtils.copyBytes(in, baos, 4096, false);
			n = Integer.parseInt(baos.toString().replace("\n", ""));
			System.out.println("End Of file: HDFS file read complete");

		} finally {
			IOUtils.closeStream(in);
			
		}
	}

	public static boolean getMergeInHdfs(FileSystem fs, String src, String dest)
			throws IllegalArgumentException, IOException {
		Path srcPath = new Path(src);
		Path dstPath = new Path(dest);

		if (!fs.exists(srcPath))
			throw new FileNotFoundException(src + "not present");

		return FileUtil.copyMerge(fs, srcPath, fs, dstPath, true, conf, null);
	}

	public int getN() {
		return n;
	}

}
