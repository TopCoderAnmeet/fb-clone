package edu.upenn.nets212.hw3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IterReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text vertex, Iterable<Text> info, Context context)
			throws IOException, InterruptedException {
		// double d = 0.15; // when to use decay?
		String output = vertex.toString() + " ";
		System.out.println("Node: " + output);
		String neighbors = "";
		// mapping labels to ranks to avoid duplication
		Map<String, Double> ranks = new HashMap<String, Double>();
		for (Text i : info) {
			String[] parts = i.toString().split(" ");
			// handling the adjacency list
			if (parts[0].equals("A")) {
				System.out.println("Adj list found for: " + vertex.toString());
				neighbors = parts[1];
				// handling labels
			} else {
				String[] label_parts = i.toString().split(",");
				String node_name = label_parts[0];
				Double r = Double.parseDouble(label_parts[1]);
				if (ranks.containsKey(node_name)) {
					Double map_rank = ranks.get(node_name);
					ranks.put(node_name, map_rank + r);
				} else {
					ranks.put(node_name, r);
				}
			}
		}
		for (String s : ranks.keySet()) {
			output += s + "," + ranks.get(s) + ":";
		}
		System.out.println(vertex.toString() + " Neighbors: " + neighbors);
		context.write(vertex, new Text(output + " " + neighbors));
	}
}
