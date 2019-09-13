package com.cc.mapreduce3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class ipCounter { 

	 public static class ipCounterMapper extends Mapper<Object, Text, Text, IntWritable>{
		 private final static IntWritable one = new IntWritable(1);
		 private Text word = new Text();
		 
		 public void map(Object key, Text value, Context context) 
				 throws IOException, InterruptedException {
			 String textContent = value.toString();
			 String[] textLines = textContent.split("\\s*\\r?\\n\\s*");
			 for(String s: textLines){
				 int end = s.indexOf(' ');
				 word.set(textContent.substring(0, end).trim());
				 context.write(word, one); 
			 }
		 }
	 } 
	 
	 public static class ipCounterReducer 
	 	extends Reducer<Text,IntWritable,Text,IntWritable> {
		 
		 private IntWritable result = new IntWritable();
		 private final static String keyIP = "10.153.239.5";
		 
		 public void reduce(Text key, Iterable<IntWritable>values, Context context) 
			throws IOException, InterruptedException {
			 	if(key.toString().trim().equals(keyIP)){
					int sum = 0;
					for (IntWritable val : values) {
						sum += val.get();
					}
					result.set(sum);
					context.write(key, result);
				}
		 }
	 } 
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
            System.err.printf("Usage: ipCounter needs 2 arguments: input and output files\n");
            return;
        }
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ipCounter");
        job.setJarByClass(ipCounter.class);
        job.setMapperClass(ipCounterMapper.class);    
        job.setReducerClass(ipCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
