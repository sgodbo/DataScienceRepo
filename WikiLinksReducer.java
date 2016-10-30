package com.shan.idstut;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WikiLinksReducer extends Reducer<Text, Text, Text, Text>{
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			String sum = "";
			for(Text val : values){
				sum += val+",";
			}
			Text t = new Text(sum);
			context.write(key, t);
		}
	}