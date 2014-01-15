package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text info, Context context)
			throws IOException, InterruptedException {
		String[] parts = info.toString().split("\t")[1].split(" ");
		System.out.println("1");
		if (parts[1].equals("nill")) {
			// emit the adjacency list
			//context.write(new Text(parts[0]), new Text("A " + parts[2]));
		} else {
			String[] labels = parts[1].split(":");
			String[] neighbors = parts[2].split(":");
			for (String l : labels) {
				if (!(l.equals("") || l.equals(" "))) {
					String lid = l.split(",")[0];
					double lw = Double.parseDouble(l.split(",")[1]);
					for (String n : neighbors) {
						String nid = n.split(",")[0];
						double nw = Double.parseDouble(n.split(",")[1]);
						double newRank = lw * nw;
						context.write(new Text(nid), new Text(lid + ","
								+ newRank));
					}
				}
			}
		}
		// emit the adjacency list of each
		System.out.println("Node is " + parts[0]);
		System.out.println("Parts2: (Neighbors) : " + parts[2]);
		context.write(new Text(parts[0]), new Text("A " + parts[2]));
	}

}
