import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
//import java.util.ArrayList;
//import java.util.Collections;
import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;

 
@SuppressWarnings("deprecation")
public class AvgMonthly {
	//JOB1
	public static class MapJoinMapper extends Mapper<LongWritable, Text, Text, Text>{
		Text outkey = new Text();
		Text outvalue = new Text();
		HashMap<Integer,String> userdata=new HashMap<Integer,String>();
		HashMap<String,String> userdata1=new HashMap<String,String>();
		protected void setup(Context context) throws IOException{
			Path[] files=DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path file : files) {
				if(file.getName().equals("WeatherStationLocations.csv")) {
					BufferedReader reader =new BufferedReader(new FileReader(file.toString()));
					String line =reader.readLine();
					int id1=0;
					while(line != null)
					{
						if(line.contains("\"USAF\",\"WBAN\",\"STATION NAME\",\"CTRY\",\"STATE\",\"LAT\",\"LON\",\"ELEV(M)\",\"BEGIN\",\"END\""))
						{
						    line=reader.readLine();
							continue;
						}
						else {
							String[] line1= line.toString().split(","); 
							//String cols[] = str.split(","); 
							String id=line1[0].replaceAll("\"","");
							if(id.substring(0,1).contains("A"))
								break;
							else
							{
							//String place=cols[2];
							id1=Integer.parseInt(id);
							String state=line1[4].replaceAll("\"","");
							//userdata.put(userid,place);
							userdata.put(id1,state);
							line=reader.readLine();
						}
						
					}
					}	
					reader.close();
				}
			}
		}
	public void map(LongWritable key ,Text value, Context context) throws IOException, InterruptedException {
		String value1=value.toString().replaceAll("\\s+"," ");
		if(value1.contains("STN--- WBAN YEARMODA TEMP DEWP SLP STP VISIB WDSP MXSPD GUST MAX MIN PRCP SNDP FRSHTT"))
			return;
		else
		{
		String cols[]=value1.toString().split(" ");
		int id= Integer.parseInt(cols[0]);
		
		String date=cols[2].substring(4,6);
		String temp=cols[3];
		//String place= userdata.get(userid);
		String state=userdata.get(id);
		if(state==null) {
			return;
			}
		outkey.set(state+" "+date);
		outvalue.set(temp);
		context.write(outkey,outvalue);
		}
		
	}
	}
	
	public static class mapReducer extends Reducer <Text,Text,Text,Text >
	 {
	public void reduce(Text key,  Iterable<Text>values, Context context) throws IOException,InterruptedException
	         
	{  

		Text outvalue=new Text(); 
		
		//outvalue.set(key);
		//outvalue1.set(values);
		Text outkey = new Text();
		//String vall=null;
		double val3=0;
		//Iterator<Text> iterator = values.iterator(); //Iterating
		int count=0;
		//double value = 0;
		for(Text value: values) {
			String value1=value.toString();
			//String val1=value1[1];
			String val2=value1;
			//vall=val2;
			//val3=Double.parseDouble(val1);
			val3=val3+Double.parseDouble(val2);
			count=count+1;
		}
		double avg=val3/count;
		outvalue.set(String.valueOf(avg));
		outkey.set(key);
		context.write(outkey, outvalue);
		/*	if(val>max){ //Finding max value

		max = val;
		a1=val2;}}
		while (iterator.hasNext()) {
		//hash.put(value, key1[1]);
		//hash1.put(value, key1[1]);
		value=value+iterator.next().get();
		count=count+1;
		} 
		double avg=value/count;
		 
		outvalue.set(String.valueOf(avg));
		 outkey.set(key);
		context.write(outkey, outvalue);
		//context.write(outkey,outvalue1);*/
	            
	            }                                             
	}
	//JOB2
	
	public static class AverageMapper extends Mapper <LongWritable, Text, Text,Text>
	{
		Text outkey = new Text();
		//DoubleWritable outvalue = new DoubleWritable();
		Text t1 = new Text();
		//static HashMap<Double,Integer> last = new HashMap<Double,Integer>();
		
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	            {
	                        //Text outvalue=new Text();
							String value1=value.toString().replaceAll("\\s+"," ");
							String[] line = value1.split(" ");
	                        	t1.set(line[0]);
	                        	context.write(t1,new Text(line[1]+" "+line[2]));
	                        	
                                       
	            }
	//private static HashMap<Double,Integer> getoutput(){
		//return last;
	//}
	}
	
	public static class maxminReducer extends Reducer <Text, Text,Text,Text >
	 {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException
	         
	{  
		
			
			Text outvalue=new Text();
			Text outkey=new Text();
			outkey.set(key);
			//outvalue.set(values);
			
			double min = Integer.MAX_VALUE;
			double max = 0;
			double val=0;
			String a = null;
			String a1 = null;
			//Iterator<DoubleWritable> iterator = values.iterator(); //Iterating
			//hash.put(value, key1[1]);
			//hash1.put(value, key1[1]);
			for(Text value: values) {
				String[] value1=value.toString().split(" ");
				String val1=value1[1];
				String val2=value1[0];
				val=Double.parseDouble(val1);
				if (val < min) { //Finding min value

			min = val;
			a=val2;

			}
			
				if(val>max){ //Finding max value

			max = val;
			a1=val2;

			}}
			//AvgMonthly obj= new AvgMonthly();
			//String s=obj.hash.get(max);s
			//String s1=obj.hash.get(min);
			 String number1=String.valueOf(max);
			String number2=String.valueOf(min);
			String number3=String.valueOf(max-min);
			String b= null;
			String b1=null;
			if(a1.contains("01"))
				b1="JAN";
			if(a1.contains("02"))
				b1="FEB";
			if(a1.contains("03"))
				b1="MAR";
			if(a1.contains("04"))
				b1="APR";
			if(a1.contains("05"))
				b1="MAY";
			if(a1.contains("06"))
				b1="JUN";
			if(a1.contains("07"))
				b1="JUL";
			if(a1.contains("08"))
				b1="AUG";
			if(a1.contains("09"))
				b1="SEP";
			if(a1.contains("10"))
				b1="OCT";
			if(a1.contains("11"))
				b1="NOV";
			if(a1.contains("12"))
				b1="DEC";
			if(a.contains("01"))
				b="JAN";
			if(a.contains("02"))
				b="FEB";
			if(a.contains("03"))
				b="MAR";
			if(a.contains("04"))
				b="APR";
			if(a.contains("05"))
				b="MAY";
			if(a.contains("06"))
				b="JUN";
			if(a.contains("07"))
				b="JUL";
			if(a.contains("08"))
				b="AUG";
			if(a.contains("09"))
				b="SEP";
			if(a.contains("10"))
				b="OCT";
			if(a.contains("11"))
				b="NOV";
			if(a.contains("12"))
				b="DEC";
			outvalue.set(number1+","+b1+" "+number2+","+b+" "+number3);
			context.write(key, outvalue);
			//outvalue.set(number2+" "+a);
			//context.write(key, outvalue);
			
			
		
	}}
		
		
	//JOB3
	//HashMap<Double,String> hash=new HashMap<Double,String>();
	public static class FinalMapper extends Mapper <LongWritable, Text, DoubleWritable,Text>
	{
		Text outkey = new Text();
		Text outvalue = new Text();
		DoubleWritable t1 = new DoubleWritable();
		
		
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	            {
	                        String value1=value.toString().replaceAll("\\s+"," ");
							String[] line = value1.split(" ");
							//AvgMonthly obj= new AvgMonthly();
							//obj.hash.put(Double.parseDouble(line[2]), line[1]);
							//hash1.put(Double.parseDouble(line[2]), line[1]);
							
							
							//t1.set(new DoubleWritable(Double.parseDouble(line[5])));
	                        	outvalue.set(line[0]+" "+line[1]+" "+line[2]);
								context.write(new DoubleWritable(Double.parseDouble(line[3])),outvalue);
	                        	//context.write(t1, new DoubleWritable(Double.parseDouble(line[3])));
                                       
	            }
	}
	
	public static class FinalReducer extends Reducer <Text,DoubleWritable,DoubleWritable,Text >
	 {
	public void reduce(DoubleWritable key, Text values, Context context) throws IOException,InterruptedException
	         
	{  
		
		context.write(key, values);
		
		/*Text outkey = new Text();
		//String[] key1=key.toString().split(" ");
		//outkey.set(key1[0]);
		//HashMap<Double,String> hash=new HashMap<Double,String>();
		//HashMap<Double,String> hash1=new HashMap<Double,String>();
		Text outvalue=new Text();  
		String value1=null;
		double val=0;
		ArrayList<Double> array=new ArrayList<Double>();
		for(Text value: values) {
			value1=value.toString();
			val=Double.parseDouble(value1);
			array.add(val);
		}
		Collections.sort(array);
		double voi = 0;
		for (int counter = 0; counter < array.size(); counter++) { 		      
	          double var= array.get(counter); 
	          voi=var;
	      } 
		 outvalue.set(String.valueOf(voi));
 		 outkey.set(key);
 		context.write(outkey, outvalue);*/

		/*outvalue.set(String.valueOf(var));
		 outkey.set(key);
		context.write(outkey, outvalue);*/
		}
		}
	
	
	
	
	
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException{
		Configuration conf= new Configuration();
		Job job =Job.getInstance(conf);
		job.setJarByClass(AvgMonthly.class);
		job.setMapperClass(MapJoinMapper.class);
		job.setReducerClass(mapReducer.class);
		job.setNumReduceTasks(1);
		//job.setNumReduceTasks(0);
		DistributedCache.addCacheFile(new URI("file:///C:/WeatherStationLocations.csv"), job.getConfiguration());
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		Path outputPath=new Path("first mapper");
		FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, outputPath);
			outputPath.getFileSystem(conf).delete(outputPath);
		//System.exit(job.waitForCompletion(true)?0:1);
		job.waitForCompletion(true);
		Configuration conf1= new Configuration();
		Job job1 =Job.getInstance(conf1);
		job1.setJarByClass(AvgMonthly.class);
		job1.setMapperClass(AverageMapper.class);
		job1.setReducerClass(maxminReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		Path outputPath1=new Path("Secondmapper");
		FileInputFormat.addInputPath(job1, outputPath);
		FileOutputFormat.setOutputPath(job1, outputPath1);
		 outputPath1.getFileSystem(conf1).delete(outputPath1, true);
		 System.exit(job1.waitForCompletion(true)?0:1);
		job1.waitForCompletion(true);
		Configuration conf2= new Configuration();
		Job job2 =Job.getInstance(conf2);
		job2.setJarByClass(AvgMonthly.class);
		job2.setMapperClass(FinalMapper.class);
		job2.setReducerClass(FinalReducer.class);
		//job2.setNumReduceTasks(2);
		job2.setOutputKeyClass(DoubleWritable.class);
		job2.setOutputValueClass(Text.class);
		Path outputPath2=new Path(args[1]);
		FileInputFormat.addInputPath(job2, outputPath1);
		FileOutputFormat.setOutputPath(job2, outputPath2);
		 outputPath2.getFileSystem(conf2).delete(outputPath2, true);
		System.exit(job2.waitForCompletion(true)?0:1);
		 //job2.waitForCompletion(true);
		
	}
}


