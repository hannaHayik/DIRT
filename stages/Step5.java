import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Step5 {
	public static int b = 0;

	public static String whichSlot(String slot) {
		if(slot.startsWith("N"))
			return "X";
		return "Y";
	}
	
	//Using this function will cause Iterable<Text>values to be NULL (it's an iterator so it's a single pass on the values)
	public static String printerHanna(Iterable<Text> values) {
		String str = "";
		for (Text val : values)
			str += val.toString() + " H5 ";
		return str;
	}

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {		

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	// path,slot,word	mi(p,s,w_)
			try {
				String[] tmp = value.toString().split("\t");
				String myKey = tmp[0];
				String miValue = tmp[1];
				String[] myVars = myKey.split(",");
				
				context.write(new Text(myVars[0]), new Text(myVars[2] + "," + whichSlot(myVars[1]) + "," + miValue));	//path		word,slot,mi(p,s,w)
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		int count = 0;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			String str = "";
			try {
				for(Text val : values)
					str = str + val.toString() + "!";
				
				context.write(key, new Text(str.substring(0, str.length()-1)));		//path		"word,slot,mi()!word,slot,mi()!word,..."
			}
			catch (Exception e) {
				e.printStackTrace();
			}	
		}
	}

	public static class PartitionerClass extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return Math.abs(((key.toString().hashCode()) % numPartitions));
		}
	}

	public static class Combiner extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			try {
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}


}
