package raghav.hadoop.learning.second;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WordCountReducer extends MapReduceBase
implements Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
		      OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		int count = 0;
		IntWritable value;
		
		while(values.hasNext())
		{
			value = values.next();
			count += value.get();
			//count=count+values.;
			//values.next();
		}
		output.collect(key , new IntWritable(count));
	}


}
