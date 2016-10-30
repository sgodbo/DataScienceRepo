package com.shan.idstut;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class PageRank {

	static Configuration conf;
	static String input_path;
	static String output_path;
	static String working_fs;

	public static class DoubComparator extends WritableComparator {

		public DoubComparator() {
			super(DoubleWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			Double v1 = ByteBuffer.wrap(b1, s1, l1).getDouble();
			Double v2 = ByteBuffer.wrap(b2, s2, l2).getDouble();

			return v1.compareTo(v2) * (-1);
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * @throws URISyntaxException
	 */
	public static void main(String[] args) throws IllegalArgumentException,
			IOException, ClassNotFoundException, InterruptedException,
			URISyntaxException {
		// TODO Auto-generated method stub
		working_fs = args[0];
		input_path = args[1];
		output_path = args[2];
		
		long start_time = System.currentTimeMillis();

		setGlobalConfigForJobs();

		final JobControl control = new JobControl("page_rank_prep_jobs");

		Job[] linkedJobs = new Job[8];
		ControlledJob[] controlledJobs = new ControlledJob[8];

		for (int i = 0; i < 8; i++) {
			linkedJobs[i] = Job.getInstance(conf, "calc_pr_one_itr" + i);
			controlledJobs[i] = new ControlledJob(linkedJobs[i], null);
			control.addJob(controlledJobs[i]);
			conf.setInt("iter", i);
			if (i != 0) {
				controlledJobs[i].addDependingJob(controlledJobs[i - 1]);
			}

			linkedJobs[i] = setPRJobProperties(linkedJobs[i], i);

			

		}

		Thread t = new Thread() {
			public void run() {
				control.run();
			}
		};

		t.start();
		while (!control.allFinished()) {
		}
		
		try {
			getMergeInHdfs(working_fs, output_path + "/temp/iter1/unsorted/filtered",
					output_path + "/temp/iter1/sorted/iter1.out");
			getMergeInHdfs(working_fs, output_path + "/temp/iter8/unsorted/filtered",
					output_path + "/temp/iter8/sorted/iter8.out");
		} catch (FileNotFoundException exp) {
			exp.printStackTrace();
		}
		
		runSortJob(1);
		runSortJob(8);
		
		try {
			getMergeInHdfs(working_fs, output_path + "/temp/iter1/sorted/merged",
					output_path + "/iter1.out");
			getMergeInHdfs(working_fs, output_path + "/temp/iter8/sorted/merged",
					output_path + "/iter8.out");
		} catch (FileNotFoundException exp) {
			exp.printStackTrace();
		}

		
		System.out.println("jobs finished");
		
		System.out.println("elapsed time ---------------->" + (System.currentTimeMillis()-start_time));
		System.exit(0);
	}

	private static void runSortJob(int iterIdx)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(conf, "sort_job");
		job.setJarByClass(PageRankSortJobs.class);
		
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(output_path + "/temp/iter"
				+ iterIdx + "/sorted/iter"+ iterIdx +".out"));

		//TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(output_path+"/temp/partition_file"));
		// Write partition file with random sampler
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		
        //InputSampler.Sampler<DoubleWritable, NullWritable> sampler = new InputSampler.RandomSampler<DoubleWritable,NullWritable>(0.01, 1000, 10);
        //InputSampler.writePartitionFile(job, sampler);

        // Use TotalOrderPartitioner and default identity mapper and reducer 
        //job.setPartitionerClass(TotalOrderPartitioner.class);
        
        
		job.setMapperClass(PageRankSortJobs.PRSortMapper.class);
		job.setReducerClass(PageRankSortJobs.PRSortReducer.class);

		
		
		
        
		job.setSortComparatorClass(DoubComparator.class);

		
		FileOutputFormat.setOutputPath(job, new Path(output_path + "/temp/iter"
				+ iterIdx + "/sorted/merged"));
		
		
		job.waitForCompletion(true);
	}

	private static Job setPRJobProperties(Job job, int jobIdx)
			throws IllegalArgumentException, IOException {
		// TODO Auto-generated method stub
		job.setJarByClass(PageRank.class);
		job.setMapperClass(PageRankMapperReducer.PageRankMapper.class);
		job.setReducerClass(PageRankMapperReducer.PageRankReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		if (jobIdx == 0)
			FileInputFormat.addInputPath(job, new Path(input_path + "/part*"));
		else
			FileInputFormat.addInputPath(job, new Path(output_path
					+ "/temp/iter" + jobIdx + "/unsorted/part*"));
		FileOutputFormat.setOutputPath(job, new Path(output_path + "/temp/iter"
				+ (jobIdx + 1) + "/unsorted/"));
		MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class,
				DoubleWritable.class, Text.class);

		return job;
	}

	private static void setGlobalConfigForJobs() throws ClassNotFoundException,
			IOException, InterruptedException, URISyntaxException {
		// TODO Auto-generated method stub
		PageRankPrepJobs p1 = new PageRankPrepJobs(working_fs, input_path,
				output_path);
		int N = p1.n;
		System.out.println(N);
		System.out.println((double) 5 / N);

		conf = new Configuration();
		conf.setInt("num_of_nodes", N);
		conf.setDouble("beta", 0.85);
		conf.setDouble("const_factor", (double) 0.15 / N);
		conf.setDouble("threshold_value", (double) 5 / N);
	}
	
	public static boolean getMergeInHdfs(String working_fs, String src,
			String dest) throws IllegalArgumentException, IOException {
		FileSystem fs;
		try {
			fs = FileSystem.get(new URI(working_fs), conf);
		} catch (URISyntaxException exp) {
			fs = FileSystem.get(conf);
			exp.printStackTrace();
		}
		Path srcPath = new Path(src);
		Path dstPath = new Path(dest);

		if (!fs.exists(srcPath))
			throw new FileNotFoundException(src + "not present");

		return FileUtil.copyMerge(fs, srcPath, fs, dstPath, false, conf, null);
	}
}
