package com.shan.idstut;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

public class RedLinksRemove {
	public static class WikiLinkMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String tagText = value.toString();
			Text title = new Text();
			Text link = new Text();
			SAXReader saxReader = new SAXReader();
			try {
				Document document = saxReader.read(new ByteArrayInputStream(tagText.getBytes()));
				Node pageNode = document.selectSingleNode("/page");
				Node titleNode = pageNode.selectSingleNode("title");
				Node textNode = pageNode.selectSingleNode("revision/text");
				title.set(titleNode.getText().replace(' ', '_'));
				Pattern p2 = Pattern.compile("\\[\\[(.*?)(\\||\\]\\])");
				String textNodeStr = textNode.getText();
				Matcher m2 = p2.matcher(textNodeStr);
				context.write(title, new Text("#"));
				while (m2.find()) {
					String temp = m2.group(1);
					String primaryTitle = "";
					if (temp.contains("|")) {
						String[] arr = temp.split("\\|");
						primaryTitle = arr[0];
					} else {
						primaryTitle = temp;
					}
					String underScoredTitle = primaryTitle.replace(' ', '_');
					link.set(underScoredTitle);
					context.write(link, title);
				}
			} catch (DocumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public static class WikiLinkReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			boolean redLink = true;
			Set<String> adjLinks = new HashSet<String>();
			for (Text val : values) {
				if (val.toString().equals("#"))
					redLink = false;
				adjLinks.add(val.toString());
			}

			if (!redLink) {
				for (String val : adjLinks) {
					if (val.toString().equals("#"))
						context.write(key, new Text(val));
					else
						context.write(new Text(val), key);
				}
			}
		}
	}

	public static class AdjGraphMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String val = value.toString();
			String[] valArr = val.split("[\\s]+");
			context.write(new Text(valArr[0]), new Text(valArr[1]));
		}
	}

	public static class AdjGraphReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String links = "";
			// Set<Text> setOfLinks = Sets.newHashSet(values);
			for (Text val : values) {
				if (!val.toString().equals("#"))
					links += val.toString() + "\t";
			}
			context.write(key, new Text(links));
		}

	}

	public static void main(String[] args) {

		try {
			Configuration conf = new Configuration();
			conf.set("xmlinput.start", "<page>");
			conf.set("xmlinput.end", "</page>");
			final JobControl control = new JobControl("RedLinkJobs");
			Job job1 = Job.getInstance(conf, "red_link_removal");

			ControlledJob cJob1 = new ControlledJob(job1, null);
			control.addJob(cJob1);

			job1.setInputFormatClass(XmlInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);

			job1.setJarByClass(RedLinksRemove.class);
			job1.setMapperClass(WikiLinkMapper.class);
			job1.setReducerClass(WikiLinkReducer.class);

			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job1, new Path(args[0]));

			FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/temp"));

			Job job2 = Job.getInstance(conf, "adj_graph_creation");
			ControlledJob cJob2 = new ControlledJob(job2, null);
			control.addJob(cJob2);
			cJob2.addDependingJob(cJob1);

			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			job2.setJarByClass(RedLinksRemove.class);
			job2.setMapperClass(AdjGraphMapper.class);
			job2.setReducerClass(AdjGraphReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job2, new Path(args[1] + "/temp/part*"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/graph"));

			Thread t = new Thread(){
				public void run(){
					control.run();
				}};
			
			t.start();
			while (!control.allFinished()) {}
			System.out.println("jobs finished");
			System.exit(0);

		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}