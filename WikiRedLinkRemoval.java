package com.shan.idstut;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


public class WikiRedLinkRemoval {

	public static class TokenizerMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			 
	        try {
	            InputStream is = new ByteArrayInputStream(value.toString().getBytes());
	            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
	            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
	            Document doc = dBuilder.parse(is);
	            doc.getDocumentElement().normalize();
	            NodeList nList = doc.getElementsByTagName("page");
	            for (int temp = 0; temp < nList.getLength(); temp++) {
	                Node nNode = nList.item(temp);
	                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
	                    Element eElement = (Element) nNode;
	                    String titlestr = eElement.getElementsByTagName("title").item(0).getTextContent().trim();
	                    titlestr = titlestr.toLowerCase().replaceAll("[( )*\\t]", "_");
	                    Text title = new Text(titlestr);
	                    String text = eElement.getElementsByTagName("text").item(0).getTextContent();
	                    Pattern pattern = Pattern.compile("\\[\\[(.*?)(\\||\\]\\])"); 
	                    Matcher matcher = pattern.matcher(text);
	                    Text match=new Text();
	                    Text marker = new Text("title");
	                    context.write(title, marker);
	                    while (matcher.find()) {
	                    	String matchStr = matcher.group(1).trim().toLowerCase().replaceAll("[( )*\\t]", "_");
	                    	if(matchStr.contains("|"))
	                    		matchStr = matcher.group(1).split("\\|")[0].toLowerCase();
	                    	if(!matchStr.equalsIgnoreCase(titlestr)) {
	                    		match.set(matchStr);
	                    		context.write(match, title);
	                    	}
	                    }
	                }
	            }
	        } catch (Exception e) {
	            System.out.println(e.getMessage());
	        }
	 
	    }
	}

	public static class WikiReducer extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for (Text val : values) {
				sb.append(val.toString());
			}
			context.write(key, new Text(sb.toString().trim()));
		}
	}

	public static class WikiCombiner extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet<String> set = new HashSet<String>();
			for (Text val : values) {
					set.add(val.toString());
			}
			String keyStr = key.toString();
			System.out.println(key+"\t");
			if(set.remove("title")) {
				for (String val : set) {
					System.out.print(val.toString());
					context.write(new Text(val), new Text(keyStr+"\t"));
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		//String xmlinput = args[0];
		//String output = args[1];
		
		//String output = "/cise/homes/sgodbole/lab5_ids/output";
		//String xmlinput = "/cise/homes/sgodbole/lab5_ids/staff.xml";
//		String xmlinput = "/cise/homes/pbaheti/lab5_ids/enwiki-latest-pages-articles.xml";
		//File f = new File(output);
		//FileUtils.deleteDirectory(f);
		Configuration conf = new Configuration();
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		Job job = Job.getInstance(conf, "WikiRedLinkRemoval");
		job.setJarByClass(WikiRedLinkRemoval.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(WikiReducer.class);
		job.setCombinerClass(WikiCombiner.class);
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("staff.xml"));
		FileOutputFormat.setOutputPath(job, new Path("output" + System.currentTimeMillis()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}