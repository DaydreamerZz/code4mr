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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.org.apache.regexp.internal.recompile;

/*
 * <name score>
 */
public class SortNumbers {
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		Path outputPath = new Path(args[1]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		Job job = new Job(conf);
		job.setJobName("Data Deduplication");
		job.setJarByClass(SortNumbers.class);
		
		job.setMapperClass(Map.class);
		job.setPartitionerClass(Partition.class);
		job.setReducerClass(Reduce.class);
	
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

	public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
		
		private static IntWritable data = new IntWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			data.set(Integer.parseInt(line));
			context.write(data, new IntWritable(1));
		}
	}
	
	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private static IntWritable lineNumber = new IntWritable(1);
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			for(IntWritable val : values){
				context.write(lineNumber, key);
				lineNumber = new IntWritable(lineNumber.get() + 1);
			}
		}
	}
	
	public static class Partition extends Partitioner<IntWritable, IntWritable>{
		public static boolean flag = true;
		public int getPartition(IntWritable key, IntWritable value, int numberOfPatitions) {
			// TODO Auto-generated method stub
			if(flag){
				System.out.println("number of partitions: " + numberOfPatitions);
				flag = false;
			}
			int maxNumber = 65223;
			int bound = maxNumber / numberOfPatitions + 1;
			int keyNumber = key.get();
			for(int i = 0; i < numberOfPatitions; i++){
				if(keyNumber < bound*(i + 1) && keyNumber >= bound * i){
					return i;
				}
			}
			return -1;
		}
		
	}
}
