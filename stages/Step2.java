import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Step2 {
	
	public static String printerHanna(Iterable<Text> values) {
		String str = "";
		for (Text val: values)
			str += val.toString() + "$";
		return str;
	}

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
		int b = 0;
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 	//	p,	slot,	w,	count,  x,  y		|*,slot,*||
			try {																									
				b++;
				String myKey = value.toString().split("\t")[0];
				String starSlotStar = value.toString().split("\t")[1];
				String[] splitted = myKey.split(",");
				context.write(new Text(splitted[0] + "," + splitted[1] + ",*"), new Text(myKey + "," + starSlotStar));	//	p, slot, *		p, slot, w, count, x, y, |*,slotY,*| 
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}



	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		int b;
		int cw2;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			b = 0;
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	//	p, slot, *			p,	slot,	w,	count,  x,  y, |*,slotY,*|
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
					context.write(new Text(newKey), new Text("" + sum));	//	p,	slot,	w,	count,  x,  y, |*,slotY,*| 			|p,slot,*|
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
}
