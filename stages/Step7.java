import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Step7 {
	public static int b = 0;

	public static boolean isSlotsReversed(String path) {
		if(path.startsWith("X"))
			return false;
		return true;
	}
	
	public static String reverseSlot(String slot) {
		if(slot.equals("X"))
			return "Y";
		return "X";
	}
	
	//Using this function will cause Iterable<Text>values to be NULL (it's an iterator so it's a single pass on the values)
	public static String printerHanna(Iterable<Text> values) {
		String str = "";
		for (Text val : values)
			str += val.toString() + " * ";
		return str;
	}

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {		

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {																									// path		word,slot,mi p1@p2
				String[] tmp = value.toString().split("\t");				
				String[] arr = tmp[1].split("!");
				HashMap<String,String> pathsToCmp = new HashMap<String,String>();
				for(int i=0; i<arr.length; i++)
					if(arr[i].contains("@")){
						String[] paths = arr[i].split("@");
						pathsToCmp.put(paths[1], paths[0]);
					}
				for(String p : pathsToCmp.keySet()) {
					for(int i=0; i<arr.length; i++)
						if(!arr[i].contains("@")) {
							if(pathsToCmp.get(p).equalsIgnoreCase("1"))
								context.write(new Text(p + "@" + tmp[0]), new Text(arr[i] + "," + "2"));	//p1@p2		word,slot,mi,2
							else																			//p1@p2		word2,slot,mi,2			....
								context.write(new Text(tmp[0] + "@" + p), new Text(arr[i] + "," + "1"));	//p2@p1		word,slot,mi,1
						}																					//p2@p1		word,slot,mi,1			....
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
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	// p1@p2		word,slot,mi,p1/p2
			HashMap<String,Double> firstPathSlotX = new HashMap<String,Double>();
			HashMap<String,Double> firstPathSlotY = new HashMap<String,Double>();
			HashMap<String,Double> secondPathSlotX = new HashMap<String,Double>();
			HashMap<String,Double> secondPathSlotY = new HashMap<String,Double>();
			try {
				for(Text val : values) {
					String[] myVals = val.toString().split(",");
					if(myVals[3].equals("1"))						//add every quadruplet to the proper map (according to which path and slot)
						if(myVals[1].equals("X"))
							firstPathSlotX.put(myVals[0], Double.parseDouble(myVals[2]));
						else
							firstPathSlotY.put(myVals[0], Double.parseDouble(myVals[2]));
					else
						if(myVals[1].equals("X"))
							secondPathSlotX.put(myVals[0], Double.parseDouble(myVals[2]));
						else
							secondPathSlotY.put(myVals[0], Double.parseDouble(myVals[2]));
				}
				
				double sumP1x = 0;
				double sumP1y = 0;
				double sumP2x = 0;
				double sumP2y = 0;
				double sharedSumX = 0;
				double sharedSumY = 0;
				
				for(double v : firstPathSlotX.values())		//calculate the sigmas of sim(slot1,slot2)
					sumP1x += v;
				for(double v : firstPathSlotY.values())
					sumP1y += v;
				for(double v : secondPathSlotX.values())
					sumP2x += v;
				for(double v : secondPathSlotY.values())
					sumP2y += v;
				
				for(String item : firstPathSlotX.keySet())
					if(secondPathSlotX.containsKey(item))
						sharedSumX = sharedSumX + firstPathSlotX.get(item) + secondPathSlotX.get(item);
				for(String item : firstPathSlotY.keySet())
					if(secondPathSlotY.containsKey(item))
						sharedSumY = sharedSumY + firstPathSlotY.get(item) + secondPathSlotY.get(item);
				double sim = Math.sqrt(( (sharedSumX) / (sumP1x + sumP2x) ) * ( (sharedSumY) / (sumP1y + sumP2y) ));		//calculate similarity between paths
				String[] twoPaths = key.toString().split("@");
				if(sim != 0)
					context.write(new Text(twoPaths[0] + " " + twoPaths[1]), new Text("" + sim));
				/*else
					context.write(key, new Text("MISSING VALUES!   " + sharedSumX + ", " + sumP1x + ", " + sumP2x + ", " 
							+ sharedSumY + ", " + sumP1y + ", " + sumP2y + "  sim() = " + sim));*/
				/*count++;
				if(count>20) {
					System.out.println("REDUCER 7   key: " + key.toString() + " values:");
					for(String item : firstPathSlotX.keySet())
						System.out.print(firstPathSlotX.get(item) + " ");
					System.out.println();
					for(String item : firstPathSlotY.keySet())
						System.out.print(firstPathSlotY.get(item) + " ");
					System.out.println();
					for(String item : secondPathSlotX.keySet())
						System.out.print(secondPathSlotX.get(item) + " ");
					System.out.println();
					for(String item : secondPathSlotY.keySet())
						System.out.print(secondPathSlotY.get(item) + " ");
				}*/
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
