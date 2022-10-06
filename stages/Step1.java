import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import javax.print.DocFlavor.URL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


public class Step1 {

	public static String printerHanna(Iterable<Text> values) {
		String str = "";
		for (Text val : values)
			str = str + val.toString() + " ^ ";
		return str;
	}
	
	public static class DIRTNode {
		public String word;
		public String pos_tag;
		public String dep_label;
		public int head_index;
		public String InOrToDependency;
		public DIRTNode modefier;
		
		public DIRTNode(String tokens) {
			String[] parts = tokens.split("/");
			this.word = parts[0];
			this.pos_tag = parts[1];
			this.dep_label = parts[2];
			this.head_index = Integer.parseInt(parts[3]);
		}
		
		public DIRTNode(String[] parts) {
			this.word = parts[0];
			this.pos_tag = parts[1];
			this.dep_label = parts[2];
			this.head_index = Integer.parseInt(parts[3]);
		}
		
	}
	

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
		int b = 0;
		String[] banned_verbs = {"am", "is" , "are", "were", "be", "been", "have", "has", "had", "do", "does", "did"};
		String[] valid_relation = {"IN", "TO", "JJ" , "JJR", "JJS", "NN", "NNS", "NNP", "NNPS", "PRP", "PRP$", "RB", "RBR", 
				"RBS", "VB" , "VBD", "VBG", "VBN", "VBP", "VBZ", "WRB", };

		public HashSet<String> banned;
		public HashSet<String> valid_relations;

		protected void setup(Context context) throws IOException, InterruptedException {
			banned = new HashSet<>(Arrays.asList(banned_verbs));
			valid_relations = new HashSet<>(Arrays.asList(valid_relation));
			System.out.println("In Step1 Mapper.setup()");
		}
		
		private String[] getFirstHeadIndex(String[] biarc) {
			String[] parts = {};
			for(int i=0; i<biarc.length; i++) {
				parts = biarc[i].split("/");
				if(parts[3].equals("0"))
					return parts;
			}
			return parts;
		}
		
		/**
		 * According to tester.java tests that I conducted, my filtered lines only have 1 object & 1 subject
		 * @param objSubj the string "subj" or the string "obj"
		 * @param biarc
		 * @return	index of subject in <b>biarc if objSubj="subj", or index of object if objSubj="obj" 
		 */
		private int findIndex(String objSubj, String[] biarc) {
			String[] parts = {};
			for(int i=0; i<biarc.length; i++) {
				parts = biarc[i].split("/");
				if(parts[3].equals("0"))	//I don't want the head_node
        			continue;
				if(parts[2].contains(objSubj))
					return i;
			}
			return -1;
		}
		
        private boolean isHeadVerb (String[] biarc){
        	String[] head = getFirstHeadIndex(biarc);
        	if(head[1].startsWith("VB"))
        		return true;
        	return false;
        }
        
        private boolean hasTwoNounsMinimum(String[] biarc) {
        	int c = 0;
        	for(int i=0; i<biarc.length; i++) {
        		String[] parts = biarc[i].split("/");
        		if(parts[1].startsWith("NN") || parts[1].startsWith("PRP"))
        			c++;
        	}
        	
        	if(c>=2)
        		return true;
        	else
        		return false;
        }
        
        private boolean startsWithLetter (String str){
            //String tmp = str.toLowerCase();	//according to docx, all data are lowercased
            char c = str.charAt(0);
            return (c >='a' && c <= 'z');
        }
        
        private boolean isValidVerb(String str) {
        	return (!banned.contains(str));
        }
        
        /**
         * checks if biarc has subject & object (OTHER THAN THE head_index 0 itself,<p>
         * I concluded that from checking MANY MANY inputs, u can refer to tester.java)
         * @param biarc
         * @return	true if the biarc has subject & object other than the head_index 0 node
         */
        private boolean hasSubjObj(String[] biarc) {
        	String[] parts = {};
        	boolean flagSubj = false;
        	boolean flagObj = false;
        	for(int i=0; i<biarc.length; i++) {
        		parts = biarc[i].split("/");
        		if(parts[3].equals("0"))	//I don't wanna count the head_node
        			continue;
        		if(parts[2].contains("subj"))
        			flagSubj = true;
        		if(parts[2].contains("obj"))
        			flagObj = true;
        	}
        	return (flagSubj && flagObj);
        }
        
        /**
         * Returns yes if the word token in the biarc doesn't contain weird characters
         * @param biarc
         * @return
         */
        private boolean noCharacters(String[] biarc) {
        	String[] parts = {};
        	for(int i=0; i<biarc.length; i++) {
        		parts = biarc[i].split("/");
        		if(parts[0].contains(":") || parts[0].contains("-") || parts[0].contains(",") || parts[0].contains("@") || parts[0].contains("!"))
        			return false;
        	}
        	return true;
        }
        
        /**
         * checks if a relation is valid (according to the assignment)
         * @param s1
         * @param s2
         * @return
         */
        private boolean isValidRelation(String s1, String s2) {
        	return ((valid_relations.contains(s1)) && (valid_relations.contains(s2)));
        }
        
        /**
         * converts tag type to become like the ones in the article (dep relations)
         * @param str
         * @return
         */
        private String getTagType(String str) {
        	if(str.startsWith("VB"))
        		return "V";
        	if(str.startsWith("NN") || str.startsWith("PRP"))
        		return "N";
        	if(str.startsWith("JJ"))
        		return "AJ";	//adjective
        	if(str.startsWith("RB"))
        		return "AD";	//adverb
        	//if(str.equalsIgnoreCase("IN") || str.equalsIgnoreCase("TO"))
        	//	return "T";
        	
        	return "N";
        }
        
        /**
         * checks if the given DIRTNode is an IN / TO node
         * @param node
         * @return
         */
        private boolean isInOrToNode(DIRTNode node) {
        	if(node.pos_tag.equalsIgnoreCase("TO") || node.pos_tag.equalsIgnoreCase("IN"))
        		return true;
        	return false;
        }
        
        private String slotFinder(String path, String slot) {
        	String[] parts = path.split("-");
        	for(int i=0; i<parts.length; i++)
        		if(!parts[i].contains(":")) 
        			if(slot.equalsIgnoreCase("X"))
        				return parts[i-1];
        			else
        				return parts[i+1];
        	return "";
        }
        
        /**
         * builds concrete path from given biarc, according to the original head indexes
         * @param biarc
         * @return
         */
        private String[] buildPath(String[] biarc) {
        	String resultPath = "";
        	String[] fivePack = new String[5];
        	List<DIRTNode> path = new LinkedList<DIRTNode>();
        	List<DIRTNode> tmpObj = new LinkedList<DIRTNode>();        	
        	boolean flag = true;
        	int subjIndex = findIndex("subj", biarc);
        	int objIndex = findIndex("obj", biarc);
        	
        	if((subjIndex == -1) || (objIndex == -1))
        		return fivePack;
        	
        	DIRTNode subject = new DIRTNode(biarc[subjIndex]);
        	DIRTNode root = new DIRTNode(getFirstHeadIndex(biarc));		//we need to add this node manually to the lists, both WHILE loops stop when they detect this node
        	DIRTNode obj = new DIRTNode(biarc[objIndex]);
        	
        	path.add(subject);
        	
        	DIRTNode prev = subject;
        	DIRTNode curr;
        	while(flag) {
        		curr = new DIRTNode(biarc[prev.head_index-1]);
        		if(isInOrToNode(curr))
        			curr.modefier = prev;
        		if(isValidRelation(prev.pos_tag, curr.pos_tag)  && (curr.head_index != 0))
        			path.add(curr);
        		if(curr.head_index == 0) {
        			flag = false;
        			break;
        		}
        		prev = curr;
        	}
        	
        	tmpObj.add(obj);
        	
        	flag = true;
        	prev = obj;
        	while(flag) {
        		curr = new DIRTNode(biarc[prev.head_index-1]);
        		if(isInOrToNode(curr))
        			curr.modefier = prev;
        		if(isValidRelation(prev.pos_tag, curr.pos_tag)  && (curr.head_index != 0))
        			tmpObj.add(curr);
        		if(curr.head_index == 0) {
        			flag = false;
        			break;
        		}
        		prev = curr;
        	}
        	
        	path.add(root);
        	for(int i=tmpObj.size()-1; i>-1; i--)
        		path.add(tmpObj.get(i));
        	
        	if(path.size() < 3)
        		return fivePack;
        	
        	for(int i=0; i<path.size(); i++) {
        		DIRTNode currNode = path.get(i);
        		if(isInOrToNode(currNode)) {
        			resultPath += getTagType(path.get(i-1).pos_tag) + "::" + currNode.word + ":" + getTagType(path.get(i+1).pos_tag) + "-";
        			continue;
        		}
        		if(currNode.head_index == 0) {
        			DIRTNode tmp = path.get(i-1);
        			DIRTNode tmp2 = path.get(i+1);
        			if(!isInOrToNode(tmp))
        				resultPath += getTagType(tmp.pos_tag) + ":" + tmp.dep_label + ":" + getTagType(currNode.pos_tag) + "-";
        			resultPath += currNode.word + "-";
        			if(!isInOrToNode(tmp2))
        				resultPath += getTagType(currNode.pos_tag) + ":" + tmp2.dep_label + ":" + getTagType(tmp2.pos_tag);
        			continue;
        		}
        			
        	}
        	
        	fivePack[0] = resultPath.substring(0, resultPath.length()-1);
        	fivePack[1] = subject.word;
        	fivePack[2] = slotFinder(resultPath, "X");
        	fivePack[3] = obj.word;
        	fivePack[4] = slotFinder(resultPath, "Y");
        	
        	return fivePack;
        	
        }
        
        /**
         * returns the index of the subject in the biarc
         * @param biarc
         * @return
         */
        private int subjIndex(String[] biarc) {
        	String[] parts = {};
        	for(int i=0; i<biarc.length; i++) {
        		parts = biarc[i].split("/");
        		if(parts[3].equalsIgnoreCase("0"))
        			continue;
        		if(parts[2].contains("subj"))
        			return i;
        	}
        	return -1;
        }
        
        /**
         * returns the index of object in the biarc
         * @param biarc
         * @return
         */
        private int objIndex(String[] biarc) {
        	String[] parts = {};
        	for(int i=0; i<biarc.length; i++) {
        		parts = biarc[i].split("/");
        		if(parts[3].equalsIgnoreCase("0"))
        			continue;
        		if(parts[2].contains("obj"))
        			return i;
        	}
        	return -1;
        }
        
        /**
         * Checks if biarc has valid order for my implementation, valid is path of kind: X [IN,TO]? verb [IN/TO]? Y
         * @param biarc
         * @return
         */
        private boolean isValidOrder(String[] biarc) {
        	int subjIdx = subjIndex(biarc);
        	int objIdx = objIndex(biarc);
        	if((objIdx == -1) || (subjIdx == -1))
        		return false;
        	
        	DIRTNode subj = new DIRTNode(biarc[subjIdx]);
        	DIRTNode obj = new DIRTNode(biarc[objIdx]);
        	List<String> tmp = new LinkedList<String>();
        	boolean flag = true;
        	
        	int idx = obj.head_index;
        	while(flag) {
        		DIRTNode tmpNode = new DIRTNode(biarc[idx-1]);
        		tmp.add(biarc[idx-1]);
        		idx = tmpNode.head_index;
        		if(idx == 0)
        			flag = false;
        	}
        	
        	for(int i=0; i<tmp.size(); i++) {
        		String[] parts = tmp.get(i).split("/");
        		if(parts[0].equalsIgnoreCase(subj.word) && parts[1].equalsIgnoreCase(subj.pos_tag))
        			return false;
        	}
        	return true;
        }
        /**
         * Makes absolute path from concrete one
         * @param path
         * @return
         */
        private String makeAbsPath(String path) {
        	String result = "X ";
        	String[] parts = path.split("-");
        	for(int i=0; i<parts.length; i++) {
        		if(parts[i].contains("::")) {
        			result += (parts[i].split("::")[1]).split(":")[0] + " ";
        			continue;
        		}
        		if(parts[i].contains(":"))
        			continue;
        		if(!parts[i].contains(":"))
        			result += parts[i] + " ";
        	}
        	
        	result += "Y";
        	return result;
        }
        

		@Override
		public void map(LongWritable key, Text value, Context context) {		//bear	war/NN/nsubj/2 bear/VBP/ccomp/0 laddie/NN/dobj/2	15	1857,2	1860,2	1866,1	1880,1	1892,1	1904,5	1932,2	1962,1
			b++;
			try {																//became  in/IN/prep/5 collision/NN/pobj/1 of/IN/prep/2 christians/NNPS/pobj/3 became/VBD/nsubj/0 13
				b++;
				String[] vals = value.toString().split("\t");
				String[] biarc = vals[1].split(" ");
				String countOfBiarc = vals[2];
				if(isValidVerb(vals[0]) && startsWithLetter(vals[0]) && isHeadVerb(biarc) && hasTwoNounsMinimum(biarc) && hasSubjObj(biarc) && noCharacters(biarc) &&
						isValidOrder(biarc)) {
					String[] fivePack = buildPath(biarc);		//[path, x, slotx, y, sloty]
					String absPath = makeAbsPath(fivePack[0]);
					String key1 = absPath + "," + fivePack[2] + "," + fivePack[1] + "," + countOfBiarc + "," + fivePack[1] + "," + fivePack[3];
					String key2 = absPath + "," + fivePack[4] + "," + fivePack[3] + "," + countOfBiarc + "," + fivePack[1] + "," + fivePack[3];
					context.write(new Text("*," + fivePack[2] + ",*"), new Text(key1));							//	*,slotX,*			p,	slotX,	X,	count, x, y
					context.write(new Text("*," + fivePack[4] + ",*"), new Text(key2));							//	*,slotY,*			p,	slotY,	Y,	count, x, y
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		int b;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("Setupping reduce() step1");
			b = 0;
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	//	*,slotY,*			p,	slot,	w,	count,  x,  y
			b++;
			int sum = 0;
			HashSet<String> mySet = new HashSet<String>();
			try {
				for(Text val: values) {
					String valString = val.toString();
					mySet.add(valString);
					sum += Integer.parseInt(valString.split(",")[3]);
				}
				for(String newKey: mySet)
					context.write(new Text(newKey), new Text("" + sum));	//	p,	slot,	w,	count,  x,  y		|*,slot,*|
			}
			catch (Exception e) {
				if (b < 20) {
					e.printStackTrace();
					System.out.println("Hanna error in reduce() catch: + " + e);
					context.write(key, new Text("Stam key to continue"));
				}
			}
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
				System.out.println("Error in Step1 Combiner --> key: " + key.toString() + " values: " + printerHanna(values));
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
