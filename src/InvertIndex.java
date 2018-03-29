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
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.lang.NumberFormatException;


public class InvertIndex {
  public static class StopWords {
    protected HashMap<String, Integer> stoplist;

    // class to open the list of stop words generated in Part 1 and store them in a map
    public StopWords() {
        stoplist = new HashMap<String, Integer>();
        BufferedReader br;

        //load the stop word file
        try{
          Configuration config = new Configuration();
          FileSystem fileSystem = FileSystem.get(new URI("hdfs://namenode:9000"), config);
          Path filePath = new Path("hdfs://namenode:9000/stopwords/part-r-00000");
          FSDataInputStream fsDataInputStream = fileSystem.open(filePath);
          br = new BufferedReader(new InputStreamReader(fsDataInputStream));
        }catch(FileNotFoundException e){
          return;
        }catch(IOException e){
          return;
        }catch(URISyntaxException e){
          return;
        }
       
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

    //returns True or False of whether or not a word is a stop word
    public boolean in(String word){
      return stoplist.get(word) != null;
    }

  }

  //the map class for map reduce in hadoop
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
    private Text word = new Text();
    private Text index = new Text();
    private StopWords stop = new StopWords();

    //function for mapping words to document name and line number
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      boolean isFirst = true; 
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName(); //gets document name
      int line_num = 1;
      String sword;

      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {

        //getting the linenumber since it is the first element in the line
        if(isFirst){ 
	   sword = itr.nextToken().toString();
           try{
	
	     line_num = Integer.parseInt(sword);
           }catch(NumberFormatException e){ //somehow didn't read a number, skipping
             sword = sword;
           }

	   isFirst = false;
	   continue;
	}

        //getting the word and removing punctuation and lowercasing it
        sword = itr.nextToken().toString();

	//checking if there is a new_line
	if(sword.contains("\n"))
	    isFirst = true;

        sword = sword.replaceAll("\\s*\\p{Punct}+\\s*$", "").replaceAll("\'", "").replaceAll("\"", "").replaceAll("[()\\s-]+", "").toLowerCase();

        //replace all non-ASCII characters
        sword = sword.replaceAll("[^\\x00-\\x7F]", "");

	//skipping word if in stopword list
        if(!stop.in(sword)){
            word.set(sword);
	    index.set(fileName+":"+Integer.toString(line_num));
            context.write(word, index);
        }

      }
    }
  }

  //combines mapped document names and line numbers to a comma separated list per word
  public static class InvertReducer extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    //reducer function for combining the counts
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      //summing the counts of each word
      String sum = "";

      //iterating through the mapped keys
      for (Text val : values) {
        sum += val.toString()+",";
      }

      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "invert index");
    job.setJarByClass(InvertIndex.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(InvertReducer.class);
    job.setReducerClass(InvertReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setJar("invert.jar"); //jar file must be called invert.jar

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
