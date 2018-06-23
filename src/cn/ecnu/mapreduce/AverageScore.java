package cn.ecnu.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * <name score>
 */
public class AverageScore {
	
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
		
		Job job = new Job(conf);
		job.setJobName("Average Score");
		job.setJarByClass(AverageScore.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
	
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String input = value.toString();
			System.out.println(input);
			StringTokenizer tokenizerLine = new StringTokenizer(input, "\n"); //tokenizer for file
			
			while(tokenizerLine.hasMoreTokens()){
				String line = tokenizerLine.nextToken();
				StringTokenizer tokenizerWord = new StringTokenizer(line); // tokenizer for line
				String name = tokenizerWord.nextToken();
				String score = tokenizerWord.nextToken();
				Text nameText = new Text(name);
				context.write(nameText, new IntWritable(Integer.parseInt(score)));
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			int count = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				sum += iterator.next().get();
				count ++;
			}
			int average =(int) sum / count;
			context.write(key, new IntWritable(average));
		}
	}
}
