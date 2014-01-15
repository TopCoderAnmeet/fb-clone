package edu.upenn.nets212.hw3;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SocialRankDriver {
	
	public static void initHelp(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("Usage: Init requires 4 args");
			System.exit(-1);
		}
		if (Integer.parseInt(args[3]) < 1) {
			System.err.println("# Reducers must be positive");
			System.exit(-1);
		}
		deleteDirectory(args[2]);
		Job job1 = new Job();
		job1.setJarByClass(SocialRankDriver.class);

		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		job1.setNumReduceTasks(Integer.parseInt(args[3]));

		job1.setMapperClass(InitMapper.class);
		job1.setReducerClass(InitReducer.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.waitForCompletion(true);
	}
	
	public static void iterHelp(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("Usage: Iter requires 4 args");
			System.exit(-1);
		}
		if (Integer.parseInt(args[3]) < 1) {
			System.err.println("# Reducers must be positive");
			System.exit(-1);
		}
		Job job2 = new Job();
		job2.setJarByClass(SocialRankDriver.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		deleteDirectory(args[2]);
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.setNumReduceTasks(Integer.parseInt(args[3]));
		

		job2.setMapperClass(IterMapper.class);
		job2.setReducerClass(IterReducer.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.waitForCompletion(true);
	}
	
	public static void finishHelp(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("Usage: Iter requires 4 args");
			System.exit(-1);
		}
		deleteDirectory(args[2]);
		Job job5 = new Job();
		job5.setJarByClass(SocialRankDriver.class);

		FileInputFormat.addInputPath(job5, new Path(args[1]));
		FileOutputFormat.setOutputPath(job5, new Path(args[2]));
		job5.setNumReduceTasks(1);
		

		job5.setMapperClass(FinishMapper.class);
		job5.setReducerClass(FinishReducer.class);

		job5.setMapOutputKeyClass(DoubleWritable.class);
		job5.setMapOutputValueClass(Text.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(DoubleWritable.class);

		job5.waitForCompletion(true);
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("Author: Corey Loman (loman)");
		if (args[0].equals("composite")) {
			composite(args);
		}

		if (args[0].equals("init")) {
			initHelp(args);
			System.exit(0);
		}
		
		if (args[0].equals("iter")) {
			iterHelp(args);
			System.exit(0);
		}
		
		if (args[0].equals("finish")) {
			finishHelp(args);
			System.exit(0);
		}
	}
	
	private static void composite(String[] args) throws Exception {
		if (args.length < 7) {
			System.err.println("Usage: Composite requires 7 args");
			System.exit(-1);
		}
		// composite <inputDir> <outputDir> <intermDir1> <intermDir2> <#iterations> <#reducers>
		String[] initArgs = {args[0], args[1], args[3], args[6]};
		initHelp(initArgs);
		String[] iterArgs1 = {args[0], args[3], args[4], args[6]};
		iterHelp(iterArgs1);
		String[] iterArgs2 = {args[0], args[4], args[3], args[6]};
		iterHelp(iterArgs2);
		for (int i = 1; i < Integer.parseInt(args[5])/2; i++) {
			String[] iterArgs1while = {args[0], args[3], args[4], args[6]};
			iterHelp(iterArgs1while);
			String[] iterArgs2while = {args[0], args[4], args[3], args[6]};
			iterHelp(iterArgs2while);
		}
		String[] finishArgs = {args[0], args[4], args[2], args[6]};
		finishHelp(finishArgs);
		System.exit(0);
	}

  static void deleteDirectory(String path) throws Exception {
    Path todelete = new Path(path);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(path),conf);
    
    if (fs.exists(todelete)) 
      fs.delete(todelete, true);
      
    fs.close();
		
		
  }

}
