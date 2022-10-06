import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Step4 {
	public static int b = 0;

	//Using this function will cause Iterable<Text>values to be NULL (it's an iterator so it's a single pass on the values)
	public static String printerHanna(Iterable<Text> values) {
		String str = "";
		for (Text val : values)
			str += val.toString() + " H4 ";
		return str;
	}

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {		

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {																							//	p,	slot,	w,	count,  x,  y, |*,slotY,*|, |p,slot,*|			|*,slot,w|
				String myKey = value.toString().split("\t")[0];
				String starSlotWordStr = value.toString().split("\t")[1];
				String[] splitted = myKey.split(",");
				double pSlotWord = Double.parseDouble(splitted[3]);
				double starSlotStar = Double.parseDouble(splitted[6]);
				double pSlotStar = Double.parseDouble(splitted[7]);
				double starSlotWord = Double.parseDouble(starSlotWordStr);
				context.write(new Text(splitted[0] + "," + splitted[1] + "," + splitted[2]), new Text("" + Math.log((pSlotWord + starSlotStar) / (pSlotStar + starSlotWord))));
				//p, slot, w			mi(p,slot,w)
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
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {		// p, w, slot			mi(p,slot,w)
			double sum = 0;
			//HashSet<String> mySet = new HashSet<String>();
			try {
				b++;
				for(Text val: values) { 
					//String[] valStr = val.toString().split(",");
					sum = Double.parseDouble(val.toString());
					//mySet.add(valStr[0] + "," + valStr[1]);
				}
					
				context.write(new Text(key), new Text("" + sum));	//	p,	slot,	w			mi(p,slot,w)
			}
			catch (Exception e) {
				e.printStackTrace();
			}	
		}
	}

	public static class PartitionerClass extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			return Math.abs(((key.toString().split(",")[0].hashCode() % numPartitions)));
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
						"Error in Step4 Combiner --> key: " + key.toString() + " values: " + printerHanna(values));
			}
		}
	}


}
