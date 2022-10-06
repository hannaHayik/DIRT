import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StepsRunner {
    public static void main(String[] args) throws Exception {
        String output = "s3n://hannadirtproject/output/";
        String[] inputs = {args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10], args[11], args[12], args[13], args[14]};
        String time = args[1];
        String PosOrNeg = args[2];
        String output1 = output + "DIRTStep1Output" + time + "/";
        Configuration conf1 = new Configuration();
        conf1.set("mapreduce.map.java.opts","-Xmx4096m");		//raise JVM memory parameters to handle the big input
        conf1.set("mapreduce.reduce.java.opts","-Xmx5120m");
        conf1.set("mapreduce.map.memory.mb","5120");
        conf1.set("mapreduce.reduce.memory.mb","6144");
        System.out.println("Configuring Step 1");
        
        Job job = Job.getInstance(conf1, "Step1");
        
        for(int i=0; i<inputs.length; i++)
        	if(!inputs[i].equals("NA"))
        		FileInputFormat.addInputPath(job, new Path(inputs[i]));
        
        job.setJarByClass(Step1.class);
        job.setMapperClass(Step1.MapperClass.class);
        job.setPartitionerClass(Step1.PartitionerClass.class);
        job.setReducerClass(Step1.ReducerClass.class);
        job.setNumReduceTasks(8);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output1));
        System.out.println("Launching Step 1");
        if (job.waitForCompletion(true)) {
            System.out.println("Step 1 finished");
        } else {
            System.out.println("Step 1 failed ");
            System.out.println(job.getStatus().getFailureInfo());
        }

        System.out.println();
        String output2 = output + "DIRTStep2Output" + time + "/";
        System.out.println("output2 = " + output2);
        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.map.java.opts","-Xmx4096m");
        conf2.set("mapreduce.reduce.java.opts","-Xmx5120m");
        conf2.set("mapreduce.map.memory.mb","5120");
        conf2.set("mapreduce.reduce.memory.mb","6144");
        CounterGroup jobCounters;
        jobCounters = job.getCounters().getGroup("NCounter");
            for (Counter counter : jobCounters){
            		System.out.println("Passing " + counter.getName() + " with value " + counter.getValue() + " to step 2");
                    conf2.set(counter.getName(), "" + counter.getValue());
                }
            
        System.out.println("Configuring Step 2");
        Job job2 = Job.getInstance(conf2, "Step2");
        job2.setJarByClass(Step2.class);
        job2.setMapperClass(Step2.MapperClass.class);
        job2.setPartitionerClass(Step2.PartitionerClass.class);
        job2.setReducerClass(Step2.ReducerClass.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(8);
        FileInputFormat.setInputPaths(job2, new Path(output1));
        FileOutputFormat.setOutputPath(job2, new Path(output2));
        System.out.println("Launching Step 2");
        if (job2.waitForCompletion(true)) {
            System.out.println("Step 2 finished");
        } else {
            System.out.println("Step 2 failed ");
            System.out.println(job2.getStatus().getFailureInfo());
        }
        
        System.out.println();
        String output3 = output + "DIRTStep3Output" + time + "/";
        System.out.println("Configuring Step 3");
        Configuration conf3 = new Configuration();
        conf3.set("mapreduce.map.java.opts","-Xmx4096m");
        conf3.set("mapreduce.reduce.java.opts","-Xmx5120m");
        conf3.set("mapreduce.map.memory.mb","5120");
        conf3.set("mapreduce.reduce.memory.mb","6144");
        Job job3 = Job.getInstance(conf3, "Step3");
        job3.setJarByClass(Step3.class);
        job3.setMapperClass(Step3.MapperClass.class);
        job3.setReducerClass(Step3.ReducerClass.class);
        job3.setPartitionerClass(Step3.PartitionerClass.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setNumReduceTasks(8);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(output2));
        FileOutputFormat.setOutputPath(job3, new Path(output3));
        System.out.println("Launching Step 3");
        if (job3.waitForCompletion(true)) {
            System.out.println("Step 3 finished");
        } else {
            System.out.println("Step 3 failed ");
            System.out.println(job3.getStatus().getFailureInfo());
        }
        
        System.out.println();
        String output4 = output + "DIRTStep4Output" + time + "/";
        System.out.println("Configuring Step 4");
        Configuration conf4 = new Configuration();
        conf4.set("mapreduce.map.java.opts","-Xmx4096m");
        conf4.set("mapreduce.reduce.java.opts","-Xmx5120m");
        conf4.set("mapreduce.map.memory.mb","5120");
        conf4.set("mapreduce.reduce.memory.mb","6144");
        Job job4 = Job.getInstance(conf4, "Step4");
        job4.setJarByClass(Step4.class);
        job4.setMapperClass(Step4.MapperClass.class);
        job4.setReducerClass(Step4.ReducerClass.class);
        job4.setPartitionerClass(Step4.PartitionerClass.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setNumReduceTasks(7);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4, new Path(output3));
        FileOutputFormat.setOutputPath(job4, new Path(output4));
        System.out.println("Launching Step 4");
        if (job4.waitForCompletion(true)) {
            System.out.println("Step 4 finished");
        } else {
            System.out.println("Step 4 failed ");
            System.out.println(job4.getStatus().getFailureInfo());
        }
        
        System.out.println();
        String output5 = output + "DIRTStep5Output" + time + "/";
        System.out.println("Configuring Step 5");
        Configuration conf5 = new Configuration();
        conf5.set("mapreduce.map.java.opts","-Xmx4096m");
        conf5.set("mapreduce.reduce.java.opts","-Xmx5120m");
        conf5.set("mapreduce.map.memory.mb","5120");
        conf5.set("mapreduce.reduce.memory.mb","6144");
        Job job5 = Job.getInstance(conf5, "Step5");
        job5.setJarByClass(Step5.class);
        job5.setMapperClass(Step5.MapperClass.class);
        job5.setReducerClass(Step5.ReducerClass.class);
        job5.setPartitionerClass(Step5.PartitionerClass.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setNumReduceTasks(5);
        job5.setMapOutputValueClass(Text.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        //MultipleInputs.addInputPath(job, new Path(output4), TextInputFormat.class, Step5.MapperClass1.class);
        //MultipleInputs.addInputPath(job, new Path("s3://hannadirtproject/positive-preds.txt"), TextInputFormat.class, Step5.MapperClass2.class);
        FileInputFormat.addInputPath(job5, new Path(output4));
        //FileInputFormat.addInputPath(job5, new Path("s3://hannadirtproject/positive-preds.txt"));
        FileOutputFormat.setOutputPath(job5, new Path(output5));
        System.out.println("Launching Step 5");
        if (job5.waitForCompletion(true)) {
            System.out.println("Step 5 finished");
        } else {
            System.out.println("Step 5 failed ");
            System.out.println(job5.getStatus().getFailureInfo());
        }
        
        
        System.out.println();
        String output6 = output + "DIRTStep6Output" + time + "/";
        System.out.println("Configuring Step 6");
        Configuration conf6 = new Configuration();
        conf6.set("mapreduce.map.java.opts","-Xmx4096m");
        conf6.set("mapreduce.reduce.java.opts","-Xmx5120m");
        conf6.set("mapreduce.map.memory.mb","5120");
        conf6.set("mapreduce.reduce.memory.mb","6144");
        Job job6 = Job.getInstance(conf6, "Step6");
        job6.setJarByClass(Step6.class);
        job6.setMapperClass(Step6.MapperClass.class);
        job6.setReducerClass(Step6.ReducerClass.class);
        job6.setPartitionerClass(Step6.PartitionerClass.class);
        job6.setMapOutputKeyClass(Text.class);
        job6.setNumReduceTasks(3);
        job6.setMapOutputValueClass(Text.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);
        //MultipleInputs.addInputPath(job6, new Path(output5), TextInputFormat.class, Step6.MapperClass1.class);
        //MultipleInputs.addInputPath(job6, new Path("s3://hannadirtproject/positive-preds.txt"), TextInputFormat.class, Step6.MapperClass2.class);
        FileInputFormat.addInputPath(job6, new Path(output5));
        if(PosOrNeg.equalsIgnoreCase("Positive"))
        	FileInputFormat.addInputPath(job6, new Path("s3://hannadirtproject/positive-preds.txt"));
        else
        	FileInputFormat.addInputPath(job6, new Path("s3://hannadirtproject/negative-preds.txt"));
        FileOutputFormat.setOutputPath(job6, new Path(output6));
        System.out.println("Launching Step 6");
        if (job6.waitForCompletion(true)) {
            System.out.println("Step 6 finished");
        } else {
            System.out.println("Step 6 failed ");
            System.out.println(job6.getStatus().getFailureInfo());
        }
        
        System.out.println();
        String output7 = output + "DIRTStep7Output" + time + "/";
        System.out.println("Configuring Step 7");
        Configuration conf7 = new Configuration();
        conf7.set("mapreduce.map.java.opts","-Xmx4096m");
        conf7.set("mapreduce.reduce.java.opts","-Xmx5120m");
        conf7.set("mapreduce.map.memory.mb","5120");
        conf7.set("mapreduce.reduce.memory.mb","6144");
        Job job7 = Job.getInstance(conf7, "Step7");
        job7.setJarByClass(Step7.class);
        job7.setMapperClass(Step7.MapperClass.class);
        job7.setReducerClass(Step7.ReducerClass.class);
        job7.setPartitionerClass(Step7.PartitionerClass.class);
        job7.setMapOutputKeyClass(Text.class);
        job7.setNumReduceTasks(1);
        job7.setMapOutputValueClass(Text.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job7, new Path(output6));
        FileOutputFormat.setOutputPath(job7, new Path(output7));
        System.out.println("Launching Step 7");
        if (job7.waitForCompletion(true)) {
            System.out.println("Step 7 finished");
        } else {
            System.out.println("Step 7 failed ");
            System.out.println(job7.getStatus().getFailureInfo());
        }
        
        System.out.println("All steps are done");
    }
}