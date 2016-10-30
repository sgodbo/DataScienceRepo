package com.shan.idstut;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.ResetableIterator.EMPTY;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class PageRankMapperReducer {

	public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			int N = context.getConfiguration().getInt("num_of_nodes", 1000);
			String valString = value.toString();
			String[] nodes = valString.split("\t");
			int outBound = nodes.length - 1;
			double PR = (double) 1 / N;
			boolean prEmpty = true;
			if (isDouble(nodes[nodes.length - 1])) {
				PR = Double.parseDouble(nodes[nodes.length - 1]);
				outBound--;
				prEmpty = false;
			}

			double temp = (double) PR / outBound;
			int i = 0;

			StringBuffer sb = new StringBuffer();
			for (String node : nodes) {
				if (i != 0 && (prEmpty || (!prEmpty && i != nodes.length - 1))) {
					String opval = String.valueOf(temp);
					context.write(new Text(node), new Text(opval));
					sb.append(node);
					sb.append("\t");
				}
				i++;
			}

			context.write(new Text(nodes[0]), new Text(sb.toString()));

		}

		private boolean isDouble(String string) {
			Pattern pattern = Pattern.compile("[0-9]+[.][0-9]+(E-[1-9]+)*");
			Matcher matcher = pattern.matcher(string);
			if (matcher.find())
				return true;
			return false;
		}
	}

	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {

		MultipleOutputs<DoubleWritable, Text> mos;

		@Override
		public void setup(Context context) {
			mos = new MultipleOutputs(context);
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			double threshold = context.getConfiguration().getDouble(
					"threshold_value", 0);
			StringBuffer sBuffer = new StringBuffer();
			for (Text val : values) {
				String doubStr = val.toString();
				if (isDouble(doubStr))
					sum += Double.parseDouble(doubStr);
				else
					sBuffer.append(val.toString());
			}

			if (!key.toString().equals("") && !key.toString().isEmpty()
					&& !key.toString().equals("\t")) {
				sum = (sum * context.getConfiguration().getDouble("beta", 0.85))
						+ context.getConfiguration().getDouble("const_factor",
								0);
				sBuffer.append(String.valueOf(sum));
				context.write(key, new Text(sBuffer.toString()));
				if (sum >= threshold) {
					mos.write("text", new DoubleWritable(sum), key, "filtered/text");
				}
			}
		}

		private boolean isDouble(String string) {
			Pattern pattern = Pattern.compile("[0-9]+[.][0-9]+(E-[1-9]+)*");
			Matcher matcher = pattern.matcher(string);
			if (matcher.find())
				return true;
			return false;
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}

	}
}
