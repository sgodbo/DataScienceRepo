package com.shan.idstut;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PageRankSortJobs {
	public static class PRSortMapper extends
			Mapper<Object, Text, DoubleWritable, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] vals = value.toString().split("\t");
			context.write(new DoubleWritable(Double.parseDouble(vals[0])),
					new Text(vals[1]));
		}
	}

	public static class PRSortReducer extends
			Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		public void reduce(DoubleWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			ArrayList<String> sameOpValues = new ArrayList<String>();
			for (Text val : values) {
				sameOpValues.add(val.toString());
			}

			Collections.sort(sameOpValues, Collections.reverseOrder());

			for (String val : sameOpValues) {
				context.write(new Text(val), key);
			}
		}
	}
}
