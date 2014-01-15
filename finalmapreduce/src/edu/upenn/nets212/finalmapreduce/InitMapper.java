package edu.upenn.nets212.hw3;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable filename, Text input, Context context)
			throws IOException, InterruptedException {
		String in = input.toString();
		String[] parts = in.split(" ");
		// handling users
		if (parts[0].equals("U")) {
			// for each interest, emit (userid, interestid)
			// also emit(interestid, userid)
			String[] interests = parts[2].split(",");
			for (String i : interests) {
				context.write(new Text("U" + parts[1]), new Text(i));
				context.write(new Text(i), new Text("U" + parts[1]));
			}
			// emit (userid, networkid)
			// emit (networkid, userid)
			context.write(new Text("U" + parts[1]), new Text(parts[3]));
			context.write(new Text(parts[3]), new Text("U" + parts[1]));
			// handling friendships
		} else {
			// emit (friend1, friend2)
			// emit (friend2, friend1)
			context.write(new Text("U" + parts[1]), new Text("U" + parts[2]));
			context.write(new Text("U" + parts[2]), new Text("U" + parts[1]));
		}
	}
}
