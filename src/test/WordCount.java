package test;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	/**
	 * The map class of WordCount.
	 */
	public static class TokenCounterMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	/**
	 * The reducer class of WordCount
	 */
	public static class TokenCounterReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	/**
	 * The main entry point.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// String[] otherArgs = new GenericOptionsParser(conf,
		// args).getRemainingArgs();
		Job job = new Job(conf, "Example Hadoop 0.20.1 WordCount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenCounterMapper.class);
		job.setReducerClass(TokenCounterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/user/hduser/testdir"));
		FileOutputFormat.setOutputPath(job,
				new Path("/user/hduser/testdir-out"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}