package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinishMapper extends
		Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text info, Context context)
			throws IOException, InterruptedException {
		String[] parts = info.toString().split("\t")[1].split(" ");
		String[] labels = parts[1].split(":");
		String[] neighbors = parts[2].split(":");
		String node = parts[0];
		System.out.println("Node: " + parts[0]);
		System.out.println("Labels: " + parts[1]);
		System.out.println("Neighbors: " + parts[2]);
		if (node.contains("U")) {
			for (String l : labels) {
				boolean suggestion = true;
				String lid = l.split(",")[0];
				// checks if the label is a user's own
				if (node.equals(lid))
					suggestion = false;
				// checks if the label is one of a user's existing friends
				for (String n : neighbors) {
					String nid = n.split(",")[0];
					if (lid.equals(nid))
						suggestion = false;
				}
				if (suggestion) {
					double rank = Double.parseDouble(l.split(",")[1]) * -1;
					context.write(new DoubleWritable(rank), new Text(node + " "
							+ lid));
				}
			}
		}
	}
}
