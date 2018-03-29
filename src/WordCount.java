//J.T. Liso and Sean Whalen
//COSC 560 Programming Assignment 2

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

	public static class WordMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		//function for mapping words
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				//getting the word and removing punctuation and lowercasing it
				String sword = itr.nextToken().toString().replaceAll("\\s*\\p{Punct}+\\s*$", "").replaceAll("\'", "").replaceAll("\"", "").replaceAll("[()\\s-]+", "").toLowerCase();

				//replace all non-ASCII characters
				sword = sword.replaceAll("[^\\x00-\\x7F]", "");

				word.set(sword);

				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		//reducer function for combining the counts
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			//summing the counts of each word
			int sum = 0;

			//iterating through the mapped keys
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);

			//checking if sum is greater than stop word threshold, writing the word if so
			if(sum >= 1100)
				context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setJar("wc.jar"); //jar file must be called wc.jar

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
