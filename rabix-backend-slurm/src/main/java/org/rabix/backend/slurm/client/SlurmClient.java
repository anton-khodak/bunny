package org.rabix.backend.slurm.client;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.rabix.backend.slurm.model.SlurmJob;
import org.rabix.bindings.BindingException;
import org.rabix.bindings.Bindings;
import org.rabix.bindings.BindingsFactory;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.requirement.ResourceRequirement;
import org.rabix.common.helper.EncodingHelper;
import org.rabix.common.helper.JSONHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SlurmClient {
    private final static Logger logger = LoggerFactory.getLogger(SlurmClient.class);

    public void getState(){
        return;
    }

    public SlurmJob getJob(String slurmJobId) throws SlurmClientException{
        try {
            Runtime rt = Runtime.getRuntime();
            String command = "squeue -h -t all -j " + slurmJobId;
            // mock command
            // String result = "15     debug slurm-jo  vagrant  CD       0:00      1 server";
            //  String command = "echo " + result;
            String[] s;
            Process proc = rt.exec(command);
            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(proc.getInputStream()));
            // example output line:
            //      14     debug job-tran  vagrant   F       0:00      1 (NonZeroExitCode)
            // Explanation:
            //   JOBID  PARTITION  NAME   USER      ST       TIME  NODES NODELIST(REASON)
            String output = stdInput.readLine().trim();
            logger.debug("Pinging slurm queue: \n" + output);
            s = output.split("\\s+");
            // Mock squeue output
            // String result = "15     debug slurm-jo  vagrant  CD       0:00      1 server";
            // s = result.split("\\s+");
            String jobState = s[4];
            SlurmJob slurmJob = new SlurmJob(jobState);
            return slurmJob;

        } catch (IOException e) {
            logger.error("Could not open job file");
            throw new SlurmClientException("Failed to get ServiceInfo entity", e);

//            e.printStackTrace(System.err);
//            System.exit(10);
        }

    }

    public String runJob(Job job, File workingDir) {
        String output = "";
        String jobId = "";
        try {
            Bindings bindings = BindingsFactory.create(job);

            String slurmJobText = "#!/bin/sh\n";

            ResourceRequirement resourceRequirements = bindings.getResourceRequirement(job);
            String slurmDirective = getSlurmResourceRequirements(resourceRequirements);
            slurmJobText += slurmDirective;
            // Will be replaced when the execution side is handled
            String slurmCommand = "srun echo \"Bunny job received\"";
            slurmJobText += slurmCommand;
            logger.debug("Sending slurm job");
            Runtime rt = Runtime.getRuntime();

            // Write job.json file
            String cwlJob = EncodingHelper.decodeBase64(job.getApp());
            FileUtils.writeStringToFile(
                    workingDir,
                    JSONHelper.writeObject(cwlJob)
            );
            String command = "sbatch ";
            String s;
            // Mock command
            // s = "Submitted batch job 16";
            // String command = "echo " + s;
            Process proc = rt.exec(command);
            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(proc.getInputStream()));
            BufferedReader stdError = new BufferedReader(new
                    InputStreamReader(proc.getErrorStream()));
            int i = 0;
            while ((s = stdInput.readLine()) != null) {
                if (i == 0) {
                    // Example output (in case of success): "Submitted batch job 16"
                    // TODO: handle errors
                    String pattern = "job\\s*\\d*";
                    Pattern r = Pattern.compile(pattern);
                    Matcher m = r.matcher(s);
                    if (m.find()) {
                        jobId = m.group(0).split("\\s")[1];
                    }else {
                        logger.debug("Submission went unsuccessfully");
                    }
                    output += s;
                    i++;
                }
            }
        } catch(IOException e){
            logger.error("Could not open job file");
            e.printStackTrace(System.err);
            System.exit(10);
        }catch(BindingException e){
            logger.error("Failed to use Bindings", e);
            e.printStackTrace(System.err);
            System.exit(11);
        }
        return jobId;
    }

    private static String getSlurmResourceRequirements(ResourceRequirement requirements){
        final String batchDirective = "#SBATCH";
        String directive = "";
        if (requirements != null) {
            Long cpuMin = requirements.getCpuMin();
            Long memMin = requirements.getMemMinMB();
            if (cpuMin != null) {
                directive += batchDirective + " --ntasks-per-node=" + Long.toString(cpuMin) + "\n";
            }
            if (memMin != null) {
                directive += batchDirective + " --mem=" + Long.toString(memMin) + "\n";
            }
        }
        return directive;
    }

    // move to a separate class
    private static void writeInputs(Job job, File baseDir){
        // writes intermediate CWL inputs to
        }
    }

}
