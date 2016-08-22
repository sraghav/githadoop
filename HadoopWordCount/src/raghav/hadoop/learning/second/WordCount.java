package raghav.hadoop.learning.second;

// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class WordCount {

  /**
 * @param args
 * @throws IOException
 */
public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: WordCount <input path> <output path>");
      System.exit(-1);
    }
    
    JobConf conf = new JobConf(WordCount.class);
    conf.setJobName("Word Count");
    //conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
    //conf.set("mapreduce.jobtracker.address", "localhost:54311");
    //conf.set("mapreduce.framework.name", "yarn");
    //conf.set("yarn.resourcemanager.address", "localhost:8032");
    conf.set("mapred.reduce.tasks","16");
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setMapperClass(WordCountMapper.class);
    conf.setReducerClass(WordCountReducer.class);
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    JobClient.runJob(conf);
  }
}
// ^^ MaxTemperature