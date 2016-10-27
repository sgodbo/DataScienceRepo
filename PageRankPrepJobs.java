package com.shan.idstut;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

class PageRankPrepJobs {

	int n;

	public static class UniqNodeMapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String edgeText = value.toString();
			String[] nodesStrings = edgeText.split("\t");
			for (String node : nodesStrings) {
				context.write(new Text(node), new IntWritable(1));
			}
		}
	}

	public static class UniqNodeReducer extends
			Reducer<Text, IntWritable, IntWritable, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			context.write(new IntWritable(1), new IntWritable(1));
		}
	}

	public static class UniqNodeCountMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] vals = value.toString().split("\t");
			context.write(new Text(vals[0]), new Text(vals[1]));
		}
	}

	public static class UniqNodeCountReducer extends
			Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (Text val : values) {
				count++;
			}
			context.write(new Text("count"), new IntWritable(count));
		}
	}

	public PageRankPrepJobs() throws IOException {
		Configuration conf = new Configuration();
		final JobControl control = new JobControl("page_rank_prep_jobs");
		Job job1 = Job.getInstance(conf, "map_uniq_nodes");
		ControlledJob cJob1 = new ControlledJob(job1, null);
		control.addJob(cJob1);

		job1.setJarByClass(PageRankPrepJobs.class);
		job1.setMapperClass(UniqNodeMapper.class);
		job1.setReducerClass(UniqNodeReducer.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job1, new Path("part-0000"));
		FileOutputFormat.setOutputPath(job1, new Path("output"));

		Job job2 = Job.getInstance(conf, "count_uniq_nodes");
		ControlledJob cJob2 = new ControlledJob(job2, null);
		control.addJob(cJob2);
		cJob2.addDependingJob(cJob1);

		job2.setJarByClass(PageRankPrepJobs.class);
		job2.setMapperClass(UniqNodeCountMapper.class);
		job2.setReducerClass(UniqNodeCountReducer.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path("output"));
		FileOutputFormat.setOutputPath(job2, new Path("output@"));

		Thread t = new Thread() {
			public void run() {
				control.run();
			}
		};

		t.start();
		while (!control.allFinished()) {
		}

		System.out.println("jobs finished");

		// Get the filesystem - HDFS
		FileSystem fs = FileSystem.get(URI.create("output@/part-r-00000"), conf);
		FSDataInputStream in = null;

		try {
			// Open the path mentioned in HDFS
			in = fs.open(new Path("output@/part-r-00000"));
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			IOUtils.copyBytes(in, baos, 4096, false);
			n = Integer.parseInt(baos.toString().split("\t")[1].replace("\n", ""));
			System.out.println("End Of file: HDFS file read complete");

		} finally {
			//n = Integer.parseInt(in.readLine().split("\t")[1]);
			IOUtils.closeStream(in);
		}
	}

	public int getN() {
		return n;
	}

}
