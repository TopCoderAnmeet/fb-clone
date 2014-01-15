package edu.upenn.nets212.hw3;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text vertex, Iterable<Text> edges, Context context)
			throws IOException, InterruptedException {
		String node = vertex.toString();
		String output;
		ArrayList<String> edges_list = new ArrayList<String>();
		double edge_count = 0;
		for (Text e : edges) {
			edge_count++;
			edges_list.add(e.toString());
		}
		edge_count = (1.0 / edge_count);
		// handling users
		if (node.contains("U")) {
			output = node + " " + node + ",1.0 ";
			for (String e : edges_list) {
				String curr = e.toString();
				output = output + curr + "," + edge_count + ":";
			}
			context.write(vertex, new Text(output));
			// handling interests/networks
		} else {
			output = node + " nill ";
			for (String e : edges_list) {
				String curr = e.toString();
				output = output + curr + "," + edge_count + ":";
			}
			context.write(vertex, new Text(output));
		}
	}
}
