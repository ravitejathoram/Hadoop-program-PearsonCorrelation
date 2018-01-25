import java.util.*;
import java.io.*;
import java.text.DecimalFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;

public class PMapper extends Mapper<LongWritable, Text, Text, Text>
{	
	public void map(LongWritable key, Text value, Context contex) throws IOException, InterruptedException
	{
		String line = value.toString();
		String[] wordsinline = line.split(",");
		double o=new Double(wordsinline[4]);
		double c=new Double(wordsinline[7]);
		double p=(c-o)/o;
		p=p*100;
		DecimalFormat df = new DecimalFormat("#.##");
		p=Double.valueOf(df.format(p));
		String val=Double.toString(p);
		contex.write(new Text(wordsinline[0]),new Text(val));
	}
}

