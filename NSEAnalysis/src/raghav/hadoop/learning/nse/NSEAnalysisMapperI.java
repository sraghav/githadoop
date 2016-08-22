package raghav.hadoop.learning.nse;

import java.io.IOException;
import java.sql.Date;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NSEAnalysisMapperI extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text> {	
	
	private int findMonth(String mmm)
	{
		int month;
		switch (mmm) {
		    case "JAN"   : month=1; break;
		    case "FEB"   : month=2; break;
		    case "MAR"   : month=3; break;
		    case "APR"   : month=4; break;
		    case "MAY"   : month=5; break;
		    case "JUN"   : month=6; break;
		    case "JUL"   : month=7; break;
		    case "AUG"   : month=8; break;
		    case "SEP"   : month=9; break;
		    case "OCT"   : month=10;break;
		    case "NOV"   : month=11;break;
		    case "DEC"   : month=12;break;
		    default : month=-1;
		}
		return month;
	}
	
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		
/*

0		1		2	3	 4	 5		6	7		 	8		9		 	10		 11			12
SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN,
20MICRONS,EQ,30.8,32.4,30.7,31.15,31.35,30.75,62519,1953678.15,31-OCT-2014,444,INE144J01027,
3IINFOTECH,EQ,7.3,7.65,7.15,7.3,7.35,7.3,1517647,11154071.05,31-OCT-2014,1483,INE748C01020,
3MINDIA,EQ,6052.5,6235,6052.5,6199.8,6200,6076.95,619,3834283.35,31-OCT-2014,94,INE470A01017,
8KMILES,EQ,740,765,720,735.9,736,745.5,67398,49995316,31-OCT-2014,3749,INE650K01013,
A2ZMES,EQ,22.5,23.4,22.05,22.9,22.6,22.3,576101,13292042.5,31-OCT-2014,1818,INE619I01012,
AARTIDRUGS,EQ,722.9,728.75,715,722.15,727,708.1,8270,5958464.3,31-OCT-2014,391,INE767A01016,
AARTIIND,EQ,296,307.7,292,305.05,304.65,291.45,283461,85801686.85,31-OCT-2014,4281,INE769A01020,
AARVEEDEN,EQ,46.6,47.4,44.6,45,45.4,46.4,5300,241291.9,31-OCT-2014,97,INE273D01019,
 */
		
		String line = value.toString();
		
		String[] lineparts = line.split(",");
		
		String symbol; // = lineparts[0]; // the ticker name
		
		Float highPrice=0.0f;
		
		try
		{
		}catch(Exception e)
		{
			System.out.println("Ignoring header row..");
			System.out.println(line);
		}
		// 31 - OCT - 2014
		// 01 2 345 6 789A
		/*Integer YYYY = Integer.valueOf(lineparts[10].substring(7,10));
		int MM = findMonth(lineparts[10].substring(3,5));
		Integer DD = Integer.valueOf(lineparts[10].substring(0,1));
		
		Date highDate = new Date(YYYY.intValue(), MM, DD.intValue());
		
		for(int i=0; i<lineparts.length; ++i)
		{
			output.collect(new Text(lineparts[i]), new IntWritable(1));
		}*/
		
		//hashDtPrice.put(highDate, highPrice );
		String dateStr = lineparts[10];

		//System.err.println("Starting....");

		/*System.err.println("Current line is =>"+line);
		System.err.println("Symbol = "+symbol);
		System.err.println("date= "+dateStr);
		System.err.println("highPrice= "+highPrice);
		*/
		symbol = lineparts[0];
		if(symbol.equals(new String("SYMBOL")))
		{
			System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&Ignoring line now... "+symbol);
		}
		else
		{
			highPrice = Float.valueOf(lineparts[3]);
			String s = highPrice.toString();
			System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&Emitting line now... |"+symbol+"|"+dateStr+"|"+s+"|");
			output.collect(new Text(symbol), new Text(dateStr+"|"+s));
		}
		// ICICI 10-aug-2015 | 56.09
	}


}