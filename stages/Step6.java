import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Step6 {
	public static int b = 0;
	
	//Using this function will cause Iterable<Text>values to be NULL (it's an iterator so it's a single pass on the values)
	public static String printerHanna(Iterable<Text> values) {
		String str = "";
		for (Text val : values)
			str += val.toString() + " H6 ";
		return str;
	}
	
	public static String reversePathIfNeeded(String p) {
		if(p.startsWith("Y")) {
			String str = "X" + p.substring(1, p.length()-1) + "Y";
			return str;
		}
		return p;
	}
	
	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {	// mapper	

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	// path		"word,slot,mi!word,slot,mi ..."
			try {
				String val = value.toString();
				if(val.contains(",")) {
					String[] tmp = val.split("\t");
					String myPath = tmp[0];
					String myVal = tmp[1];
					
					context.write(new Text(myPath), new Text(myVal));		// x lead to y		word,slot,mi!word,slot,mi  ...
				}
				else {
					String[] tmp = val.split("\t");
					String path1 = reversePathIfNeeded(tmp[0]);			// x lead to y
					String path2 = reversePathIfNeeded(tmp[1]);			// x turn to y
					
					context.write(new Text(path1), new Text("1@" + path2));		// x lead to y			1@x turn to y
					context.write(new Text(path2), new Text("2@" + path1));		// x turn to y			2@x lead to y
				}
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
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {// path		word,slot,mi!p1@p2!word,slot,...	
			try {
				String str = "";
				for(Text val: values)
					str = str + val + "!";
				context.write(key, new Text(str));
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
