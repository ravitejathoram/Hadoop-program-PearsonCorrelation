import java.util.*;
import java.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PDriver extends Configured implements Tool
{
	public int run(String args[]) throws Exception
	{
		Configuration conf = this.getConf();
		Job job=Job.getInstance(conf,"xyz");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(Text.class);	
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(PMapper.class);
		job.setReducerClass(PReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));	
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setJarByClass(PDriver.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}
	public static void main(String args[]) throws Exception
	{
		int res= ToolRunner.run(new Configuration(), new PDriver(), args);
        	System.exit(res);
	}

}
