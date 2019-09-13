package com.cc.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;

public class ngramFreqCounter extends Configured implements Tool { 

	 public static class NGramMapper extends Mapper<Object, Text, Text, IntWritable>{
		 private final static IntWritable one = new IntWritable(1);
		 private Text word = new Text();
		 
		 public void map(Object key, Text value, Context context) 
				 throws IOException, InterruptedException {
			 Configuration conf = context.getConfiguration();
	         int gram_size = Integer.parseInt(conf.get("gram_size"));
			 String textContent = value.toString();
			 textContent = textContent.replaceAll("\\s+","");
			 System.out.println("Key1: "+key);
			 System.out.println("Value1: "+textContent);
			 System.out.println("gram_size: "+gram_size);
			 for(int index=0; index < textContent.length()-gram_size+1; index++){
				 word.set(textContent.substring(index, index+gram_size));
				 context.write(word, one); 
			 }
		 }
	 } 
	 
	 public static class NGramReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		 
		 private IntWritable result = new IntWritable();
		 
		 public void reduce(Text key, Iterable<IntWritable>values, Context context) 
			throws IOException, InterruptedException {
			 int sum = 0;
			 System.out.println("Key: "+key.toString());
			 for (IntWritable val : values) {
				 System.out.println("val: "+val);
				 sum += val.get();
			 }
		 System.out.println("Sum: "+sum);
		 result.set(sum);
		 context.write(key, result);
		 }
	 } 
	
	 public static void main(String[] args) throws Exception {
	        Configuration conf = new Configuration();
	        int res = ToolRunner.run(conf, new ngramFreqCounter(), args);
	        System.exit(res);

	    }
	    @Override
	    public int run(String[] args) throws Exception {
	    	if (args.length != 3) {
	            System.err.printf("Usage: ngramFreqCounter needs 3 arguments: ngramlength, input and output files\n");
	            System.exit(-1);
	        }

	        String gram_size = args[0];
	        String source = args[1];
	        String dest = args[2];
	        Configuration conf = new Configuration();
	        conf.set("gram_size", gram_size);
	        Job job = new Job(conf, "ngramFreqCounter");
	        job.setJarByClass(ngramFreqCounter.class);
	        FileSystem fs = FileSystem.get(conf);

	        Path in =new Path(source);
	        Path out =new Path(dest);
	        if (fs.exists(out)) {
	            fs.delete(out, true);
	        }

	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(IntWritable.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        job.setMapperClass(NGramMapper.class);
	        job.setReducerClass(NGramReducer.class);
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	        FileInputFormat.addInputPath(job, in);
	        FileOutputFormat.setOutputPath(job, out);
	        boolean sucess = job.waitForCompletion(true);
	        return (sucess ? 0 : 1);
	    }
}
