package com.shan.idstut;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.zookeeper.AsyncCallback.StatCallback;

import java.util.regex.Matcher;
import com.shan.idstut.PageRankPrepJobs.UniqNodeCountMapper;
import com.shan.idstut.PageRankPrepJobs.UniqNodeCountReducer;
import com.shan.idstut.PageRankPrepJobs.UniqNodeMapper;
import com.shan.idstut.PageRankPrepJobs.UniqNodeReducer;
import com.sun.xml.bind.v2.schemagen.xmlschema.List;

public class PageRank {
	public static class PRInitializerMapper extends
			Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			int N = context.getConfiguration().getInt("num_of_nodes", 5000);
			String vals = value.toString();
			String[] nodes = vals.split("\t");
			String targetVal = "";
			if(nodes[0].length() + 1 < vals.length())
				targetVal = vals.substring(nodes[0].length() + 1, vals.length());
			
			context.write(
					new Text(nodes[0]),
					new Text(targetVal
							+ "\t"
							+ String.valueOf((double) 1/N)));
		}
	}

	public static class PRInitializerReducer extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class LinkMapper extends
			Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			int N = Integer.parseInt(context.getConfiguration().get(
					"num_of_nodes"));
			String valString = value.toString();
			String[] nodes = valString.split("\t");
			int outBound = nodes.length - 2;
			double PR = Double.parseDouble(nodes[nodes.length - 1]);
			double temp = PR / outBound;
			int i = 0;
			StringBuffer sb = new StringBuffer();
			for (String node : nodes) {
				if (i != 0 && i != nodes.length - 1) {
					context.write(new Text(node), new Text(String.valueOf(temp)));
					sb.append(node);
					sb.append("\t");
				}

				i++;
			}
			
			context.write(new Text(nodes[0]), new Text(sb.toString()));
		}
	}

	public static class LinkReducer extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0;
			StringBuffer sBuffer = new StringBuffer();
			for (Text val : values) {
				if(isDouble(val.toString()))
						sum += Double.parseDouble(val.toString());
				else{
					sBuffer.append(val.toString());
				}					
			}
			
			sum = sum * context.getConfiguration().getDouble("beta", 0.85)
					+ context.getConfiguration().getDouble("const_factor", 0);
			
			if(sum >= context.getConfiguration().getDouble("threshold_value", 0)){
				sBuffer.append(String.valueOf(sum));				
				context.write(key, new Text(sBuffer.toString()));
			}			
		}

		private boolean isDouble(String string) {
			// TODO Auto-generated method stub
			Pattern pattern = Pattern.compile("[0-9]+[.][0-9]+");
			Matcher matcher = pattern.matcher(string);
			if(matcher.find())
				return true;
			return false;
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IllegalArgumentException,
			IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		PageRankPrepJobs p1 = new PageRankPrepJobs();
		int N = p1.n;
		//int N = 5;
		System.out.println(N);
		
		Configuration conf = new Configuration();
		conf.setInt("num_of_nodes", N);
		conf.setDouble("beta", 0.85);
		conf.setDouble("const_factor", (double)0.15/N);
		conf.setDouble("threshold_value", (double) 5/N);
		
		final JobControl control = new JobControl("page_rank_prep_jobs");
		Job job1 = Job.getInstance(conf, "initialize_pr");
		ControlledJob cJob1 = new ControlledJob(job1, null);
		control.addJob(cJob1);
		
		job1.setJarByClass(PageRank.class);
		job1.setMapperClass(PRInitializerMapper.class);
		job1.setReducerClass(PRInitializerReducer.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path("part-00001"));
		FileOutputFormat.setOutputPath(job1, new Path("output@@"));
		
		
		Job[] linkedJobs = new Job[8];
		ControlledJob[] controlledJobs = new ControlledJob[8];
		linkedJobs[0] = Job.getInstance(conf, "calc_pr_one_itr");
		controlledJobs[0] = new ControlledJob(linkedJobs[0], null);
		control.addJob(controlledJobs[0]);
		controlledJobs[0].addDependingJob(cJob1);

		linkedJobs[0].setJarByClass(PageRank.class);
		linkedJobs[0].setMapperClass(LinkMapper.class);
		linkedJobs[0].setReducerClass(LinkReducer.class);

		linkedJobs[0].setInputFormatClass(TextInputFormat.class);
		linkedJobs[0].setOutputFormatClass(TextOutputFormat.class);

		linkedJobs[0].setMapOutputKeyClass(Text.class);
		linkedJobs[0].setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(linkedJobs[0], new Path("output@@"));
		FileOutputFormat.setOutputPath(linkedJobs[0], new Path("output0"));
		MultipleOutputs.addNamedOutput(linkedJobs[0], "text", outputFormatClass, keyClass, valueClass)
		
		for(int i = 1;i < 3;i++){
			linkedJobs[i] = Job.getInstance(conf, "calc_pr_one_itr"+i);
			controlledJobs[i] = new ControlledJob(linkedJobs[i], null);
			control.addJob(controlledJobs[i]);
			controlledJobs[i].addDependingJob(controlledJobs[i-1]);

			linkedJobs[i].setJarByClass(PageRank.class);
			linkedJobs[i].setMapperClass(LinkMapper.class);
			linkedJobs[i].setReducerClass(LinkReducer.class);

			linkedJobs[i].setInputFormatClass(TextInputFormat.class);
			linkedJobs[i].setOutputFormatClass(TextOutputFormat.class);

			linkedJobs[i].setMapOutputKeyClass(Text.class);
			linkedJobs[i].setMapOutputValueClass(Text.class);
			FileInputFormat.addInputPath(linkedJobs[i], new Path("output"+(i-1)+"/"));
			FileOutputFormat.setOutputPath(linkedJobs[i], new Path("output"+i+"/"));
		}
		

		Thread t = new Thread() {
			public void run() {
				control.run();
			}
		};

		t.start();
		while (!control.allFinished()) {
		}
		System.out.println("jobs finished");
		System.exit(0);
	}
}
