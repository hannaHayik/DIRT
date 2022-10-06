import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Step3 {
	public static int b = 0;

	//Using this function will cause Iterable<Text>values to be NULL (it's an iterator so it's a single pass on the values)
	public static String printerHanna(Iterable<Text> values) {
		String str = "";
		for (Text val : values)
			str += val.toString() + " H3 ";
		return str;
	}

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	//	p,	slot,	w,	count,  x,  y, |*,slotY,*| 			|p,slot,*|
			try {																									
				String myKey = value.toString().split("\t")[0];
				String pSlotStar = value.toString().split("\t")[1];
				String[] splitted = myKey.split(",");
				context.write(new Text("*," + splitted[1] + "," + splitted[2]), new Text(myKey + "," + pSlotStar));		//	*, slot, w			p,	slot,	w,	count,  x,  y, |*,slotY,*|, |p,slot,*|
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
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {		//	*, slot, w			p,	slot,	w,	count,  x,  y, |*,slotY,*|, |p,slot,*|
			int sum = 0;
			try {
				b++;
				HashSet<String> mySet = new HashSet<String>();
				for(Text val: values) {
					String valString = val.toString();
					mySet.add(valString);
					sum += Integer.parseInt(valString.split(",")[3]);
				}
				for(String newKey: mySet)
					context.write(new Text(newKey), new Text("" + sum));	//	p,	slot,	w,	count,  x,  y, |*,slotY,*|, |p,slot,*|			|*,slot,w|
			}
			catch (Exception e) {
				e.printStackTrace();
			}	
		}
	}

	public static class PartitionerClass extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return Math.abs(((key.toString().split(",")[1].hashCode() % numPartitions)));
		}
	}

	public static class Combiner extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			try {
				for (Text txt : values) {
					count += Integer.parseInt(txt.toString());
				}
				context.write(key, new Text(String.valueOf(count)));
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(
						"Error in Step3 Combiner --> key: " + key.toString() + " values: " + printerHanna(values));
			}
		}
	}


}
