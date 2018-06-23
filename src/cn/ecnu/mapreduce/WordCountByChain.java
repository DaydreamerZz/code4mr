package cn.ecnu.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * <name score>
 * chain map
 */
public class WordCountByChain {
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		Path outputPath = new Path(args[1]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
	
		
		/*String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}*/
		
		DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/user/hadoop/cachefile/keyword#keyword"), conf);
				
		Job job = new Job(conf);
		job.setJobName("word count ++");
		job.setJarByClass(WordCountByChain.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
	
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class FilterMap extends Mapper<LongWritable, Text, Text, Text>{
		private final static String[] keywords = {"a", "an", "the", "oh", "to", "on", "for"};
		private HashSet<String> stopWordSet = new HashSet<String>();
		
		
		/*public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				String str = tokenizer.nextToken();
				if(keyword.contains(str))
					continue;
				word.set(str);
				context.write(word, one);
			}
		}*/
	}

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		private HashSet<String> keyword;
		private Path[] localFiles;
		
	
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				String str = tokenizer.nextToken();
				if(keyword.contains(str))
					continue;
				word.set(str);
				context.write(word, one);
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
}
