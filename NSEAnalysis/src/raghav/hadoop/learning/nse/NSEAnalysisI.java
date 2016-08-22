package raghav.hadoop.learning.nse;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.BasicConfigurator;

public class NSEAnalysisI {

  /**
 * @param args
 * @throws IOException
 */
public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: NSE Analysis <input path> <output path>");
      System.exit(-1);
    }
    
    BasicConfigurator.configure();
    
    JobConf conf = new JobConf(NSEAnalysisI.class);
    conf.setJobName("NSE Data Analysis");
    conf.set("hadoop.home.dir","/hadoop");
    //conf.set("fs.defaultFS", "hdfs://127.0.0.1:54310");
    //conf.set("mapreduce.jobtracker.address", "localhost:54311");
    //conf.set("mapreduce.framework.name", "yarn");
    //conf.set("yarn.resourcemanager.address", "localhost:8032");
    //conf.set("mapred.reduce.tasks","2");
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));;
    
    conf.setMapperClass(NSEAnalysisMapperI.class);
    conf.setReducerClass(NSEAnalysisReducerI.class);
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    JobClient.runJob(conf);
  }
}
