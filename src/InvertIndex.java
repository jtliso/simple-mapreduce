//J.T. Liso and Sean Whalen
//COSC 560 Programming Assignment 2

import java.util.HashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.net.URI;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;


public class InvertIndex {

  public static class StopWords {
    protected HashMap<String, Integer> stoplist;

    public StopWords() { 
        stoplist = new HashMap<String, Integer>();
        BufferedReader br;

        //load the stop word file
        try{
          Configuration config = new Configuration();
          FileSystem fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), config);
          Path filePath = new Path("hdfs://localhost:9000/stopwords/part-r-00000");
          FSDataInputStream fsDataInputStream = fileSystem.open(filePath);
          br = new BufferedReader(new InputStreamReader(fsDataInputStream));
        }catch(FileNotFoundException e){
          return;
        }catch(IOException e){
          return;
        }catch(URISyntaxException e){
          return;
        }
       
        //BufferedReader br = new BufferedReader(new FileReader("hdfs://localhost:9000/test/output2/part-r-00000"));
        String s;
        try{
          while ((s = br.readLine()) != null){
              String word = s.split("\\s+")[0];
              int count = Integer.parseInt(s.split("\\s+")[1]);
              stoplist.put(word, count);
          }
        }catch(IOException e){
          return;
        }

    }

    public boolean in(String word){
      return stoplist.get(word) != null;
    }

    
  }

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private StopWords stop = new StopWords();

    //function for mapping words
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   

      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        //getting the word and removing punctuation and lowercasing it
        String sword = itr.nextToken().toString().replaceAll("\\s*\\p{Punct}+\\s*$", "").toLowerCase();

        //replace all non-ASCII characters
        sword = sword.replaceAll("[^\\x00-\\x7F]", "");

        if(stop.in(sword)){
            word.set(sword);
            context.write(word, one);
        }
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
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "invert index");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setJar("invert.jar"); //jar file must be called invert.jar

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}