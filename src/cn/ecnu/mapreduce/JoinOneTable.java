package cn.ecnu.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
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
public class JoinOneTable {
	
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
		job.setJobName("one table join");
		job.setJarByClass(JoinOneTable.class);
		
		job.setMapperClass(Map.class);
//		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
	
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

	public static class Map extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String valueString = value.toString();
			String[] strings = valueString.split(" ");
			String child = strings[0];
			String parent = strings[1];
			
			context.write(new Text(child), new Text("-" + parent));
			context.write(new Text(parent), new Text("+" + child));
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			ArrayList<Text> grandparent = new ArrayList<Text>();
			ArrayList<Text> grandchild = new ArrayList<Text>();
			
			for(Text text : values){
				String s = text.toString();
				if(s.startsWith("-")){
					grandparent.add(new Text(s.substring(1)));
				}else{
					grandchild.add(new Text(s.substring(1)));
				}
			}
			
			for(int i = 0; i < grandchild.size(); i++){
				for(int j = 0; j < grandparent.size(); j++){
					context.write(grandchild.get(i), grandparent.get(j));
				}
			}
		}
	}
}
