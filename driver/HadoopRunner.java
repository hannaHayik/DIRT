import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;

public class HadoopRunner {
	private static String accessKey;
	private static String secretKey;
	private static String sessionToken;
	
	public static void loadCredentials(String path){
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();
            line = reader.readLine();
            accessKey = line.split("=", 2)[1];
            line = reader.readLine();
            secretKey = line.split("=", 2)[1];
            line = reader.readLine();
            if (line!=null) 
            	sessionToken = line.split("=", 2)[1];
            reader.close();
        }
        catch (IOException e)
        {
            System.out.println(e);
        }

    }
    public static void main(String[] args) {
    	/*
    	 * To run the application go to CMD and run the following, ex:
    	 * java -jar Mvozrot3-1.0-SNAPSHOT.jar Negative NA NA NA NA NA NA NA NA NA NA NA NA
    	 * you can replace 'Negative' by 'Positive' to run the positive predications
    	 * the 'NA' arguments are actually inputs, I'm allowing up to 12 files of input
    	 * just replace any NA with an s3 link and it should work, ex:
    	 * java -jar Mvozrot3-1.0-SNAPSHOT.jar Positive s3://hannadirtproject/projectinput/DIRTinput7 s3://hannadirtproject/projectinput/DIRTinput8 NA NA NA NA NA NA NA NA NA NA
    	 * 
    	 * Other than that, same as before: Upload the specified .jar (of the steps) to s3, and upload the positive-preds.txt or negative ones
    	 * and put valid credentials in user/home/.aws/credentials file.
    	 */
    	loadCredentials(System.getProperty("user.home") + File.separator + ".aws" + File.separator + "credentials");
    	
        AWSCredentials credentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken);
        
        final AmazonElasticMapReduce emr = AmazonElasticMapReduceClient.builder()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
        //LocalDateTime now = LocalDateTime.now();
        //HashMap<String,String> javaProps = new HashMap<String,String>();
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://hannadirtproject/stages-1.0-SNAPSHOT.jar") // This should be a full map reduce application.
                .withMainClass("StepsRunner")
                .withArgs(LocalDateTime.now().toString().replace(':', '-'), args[0], args[1], args[2], args[3], args[4], args[5]
                		, args[6], args[7], args[8], args[9], args[10], args[11], args[12]);
        StepConfig stepConfig = new StepConfig()
                .withName("stages")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(8)
                .withMasterInstanceType(InstanceType.M4Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M4Xlarge.toString())
                .withHadoopVersion("2.7.2")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("DIRT Implementation")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withLogUri("s3n://hannadirtproject/logs/")
                .withReleaseLabel("emr-5.0.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
