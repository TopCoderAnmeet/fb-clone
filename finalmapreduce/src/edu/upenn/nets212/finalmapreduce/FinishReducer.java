package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinishReducer extends
		Reducer<DoubleWritable, Text, Text, DoubleWritable> {

	public void reduce(DoubleWritable rank, Iterable<Text> vertex, Context context)
			throws IOException, InterruptedException {
		// goes through all the ranks and outputs the vertices and their ranks in order
		double r = Double.parseDouble(rank.toString());
		r = r * -1;
		for (org.w3c.dom.Text v : vertex) {
			String curr = v.toString().substring(1);
			context.write(new Text(curr), new DoubleWritable(r));
		}
	}
}
