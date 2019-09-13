package com.cc.mapreduce4;

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

public class maxURLCounter { 

	 public static class maxURLCounterMapper extends Mapper<Object, Text, Text, IntWritable>{
		 private final static char QUOTE = '"';
		 private final static char SLASH = '/';
		 private final static char SPACE = ' ';
		 private final static IntWritable one = new IntWritable(1);
		 private Text word = new Text();
		 
		 public void map(Object key, Text value, Context context) 
				 throws IOException, InterruptedException {
			 String textContent = value.toString();
			 String[] textLines = textContent.split("\\s*\\r?\\n\\s*");
			 for(String s: textLines){
				 System.out.println("String: "+s);
				 int quote = s.indexOf(QUOTE);
				 int start = s.indexOf(SLASH, quote);
				 int end = s.indexOf(SPACE, start);
				 word.set(textContent.substring(start, end).trim());
				 System.out.println("wordKey: "+word+", wordValue: "+one);
				 context.write(word, one); 
			 }
		 }
	 } 
	 
	 public static class maxURLCounterReducer 
	 	extends Reducer<Text,IntWritable,Text,IntWritable> {
		 
		 Text maxWord = new Text();
		 private int max=-1;
		 
		 public void reduce(Text key, Iterable<IntWritable>values, Context context) 
			throws IOException, InterruptedException {
					int sum = 0;
					for (IntWritable val : values) {
						sum += val.get();
					}
					if(sum > max){
						max=sum;
						maxWord.set(key);
					}
		 }
		 
		 protected void cleanup(Context context) throws IOException, InterruptedException {
			 context.write(maxWord, new IntWritable(max));
		 }
	 } 
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
            System.err.printf("Usage: maxURLCounter needs 2 arguments: input and output files\n");
            return;
        }
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "maxURLCounter");
        job.setJarByClass(maxURLCounter.class);
        job.setMapperClass(maxURLCounterMapper.class);    
        job.setReducerClass(maxURLCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
