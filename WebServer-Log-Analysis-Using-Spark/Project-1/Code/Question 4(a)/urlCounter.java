package com.cc.mapreduce2;

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

public class urlCounter { 

	 public static class URLCounterMapper extends Mapper<Object, Text, Text, IntWritable>{
		 //private final static String GET = "GET";
		 //private final static String HTTP = "HTTP";
		 private final static char QUOTE = '"';
		 private final static char SLASH = '/';
		 private final static char SPACE = ' ';
		 private final static IntWritable one = new IntWritable(1);
		 private Text word = new Text();
		 
		 public void map(Object key, Text value, Context context) 
				 throws IOException, InterruptedException {
			 System.out.println("keyMap: "+key+", valueMap: "+value);
			 String textContent = value.toString();
			 String[] textLines = textContent.split("\\s*\\r?\\n\\s*");
			 for(String s: textLines){
				 System.out.println("String: "+s);
//				 int start = s.indexOf(GET)+GET.length();
//				 int end = s.indexOf(HTTP);
				 int quote = s.indexOf(QUOTE);
				 int start = s.indexOf(SLASH, quote);
				 int end = s.indexOf(SPACE, start);
				 word.set(textContent.substring(start, end).trim());
				 System.out.println("wordKey: "+word+", wordValue: "+one);
				 context.write(word, one); 
			 }
		 }
	 } 
	 
	 public static class URLCounterReducer 
	 	extends Reducer<Text,IntWritable,Text,IntWritable> {
		 
		 private IntWritable result = new IntWritable();
		 private final static String keyURL = "/assets/img/home-logo.png";
		
		 public void reduce(Text key, Iterable<IntWritable>values, Context context) 
			throws IOException, InterruptedException {
			 	System.out.println("key: "+key+", keyURL1: "+keyURL);
				if(key.toString().trim().equals(keyURL)){
					System.out.println("HELLO!!!");
					int sum = 0;
					for (IntWritable val : values) {
						sum += val.get();
					}	
					System.out.println("keyURL2: "+keyURL);
					result.set(sum);
					System.out.println("Key: "+key+", Result: "+result);
					context.write(key, result);
				}
		 }
	 } 
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
            System.err.printf("Usage: urlCounter needs 2 arguments: input and output files\n");
            return;
        }
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "urlCounter");
        job.setJarByClass(urlCounter.class);
        job.setMapperClass(URLCounterMapper.class);    
        job.setReducerClass(URLCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
