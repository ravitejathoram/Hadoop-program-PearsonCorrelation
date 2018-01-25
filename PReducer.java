import java.util.*;
import java.io.*;
import java.text.DecimalFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

public class PReducer extends Reducer<Text, Text, Text, DoubleWritable>
{
	int h=0;
	String val1=new String("");
	Map<String,String> map=new HashMap<String,String>();  
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		val1=new String("");
		for(Text s:values)
		{
			String t=s.toString();
			if(h==0)
			val1=val1+t;
			else
			val1=val1+","+t;
			h=1;
		}
		h=0;
		map.put(key.toString(),val1);
	}
	protected void cleanup(Context context) throws IOException,InterruptedException {
		double[] x2=new double[10];
		double[] y2=new double[10];
		int i=0,j=0;
		for(Map.Entry m:map.entrySet())
		{
			i=0;j=0;
			for(Map.Entry n:map.entrySet())
			{
				if(!((m.getKey()).equals(n.getKey())))
				{
					String[] x1=(m.getValue()).toString().split(",");
					String[] y1=(n.getValue()).toString().split(",");
					for(String s:x1)
					{
						x2[i]=Double.parseDouble(s);
						i++;
					}
					i=0;
					for(String s:y1)
					{
						y2[j]=Double.parseDouble(s);
						j++;
					}
					j=0;
					double corr = new PearsonsCorrelation().correlation(x2, y2);
					DecimalFormat df = new DecimalFormat("#.##");
					corr=Double.valueOf(df.format(corr));
					corr=corr*100;
					context.write(new Text((m.getKey().toString())+","+(n.getKey().toString())),new DoubleWritable  (corr));																			
				}
			}
		}
	}
		
}

