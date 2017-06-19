package org.rabix.backend.slurm.client;

import org.apache.commons.io.FileUtils;
import org.rabix.backend.slurm.helpers.CWLJobInputsWriter;
import org.rabix.backend.slurm.model.SlurmJob;
import org.rabix.bindings.BindingException;
import org.rabix.bindings.Bindings;
import org.rabix.bindings.BindingsFactory;
import org.rabix.bindings.model.Job;
import org.rabix.bindings.model.requirement.ResourceRequirement;
import org.rabix.common.helper.EncodingHelper;
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

    public SlurmJob getJob(String slurmJobId) throws SlurmClientException {
        SlurmJob defaultSlurmJob = new SlurmJob("E");
        try {
            String command = "squeue -h -t all -j " + slurmJobId;
            // mock command
            String result = "15     debug slurm-jo  vagrant  CD       0:00      1 server";
            String mockCommand = "echo " + result;
            String[] s;

            File commandFile = new File("command.sh");
            FileUtils.writeStringToFile(commandFile, command);
            String[] commands = {"bash","command.sh"};
            ProcessBuilder pb = new ProcessBuilder(commands);
            pb.redirectErrorStream(true);
            Process p = pb.start();
            Thread.sleep(1000);
            logger.debug("Sending command: \n" + command);

            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(p.getInputStream()));
            // example output line:
            //      14     debug job-tran  vagrant   F       0:00      1 (NonZeroExitCode)
            // Explanation:
            //   JOBID  PARTITION  NAME   USER      ST       TIME  NODES NODELIST(REASON)
            String output = stdInput.readLine().trim();
            logger.debug("Pinging slurm queue: \n" + output);
            s = output.split("\\s+");
            // Mock squeue output
//             String result = "15     debug slurm-jo  vagrant  CD       0:00      1 server";
//             s = result.split("\\s+");
            String jobState = s[4];
            SlurmJob slurmJob = new SlurmJob(jobState);
            return slurmJob;

        } catch (IOException e) {
            logger.error("Could not open job file");
            throw new SlurmClientException("Failed to get ServiceInfo entity", e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return defaultSlurmJob;
    }


    public String runJob(Job job, File workingDir) {
        String output = "";
        String jobId = "";
        try {
            Bindings bindings = BindingsFactory.create(job);
            String slurmCommand = "sbatch -Q -J bunny_job_" + job.getName().replace(".", "_");
            ResourceRequirement resourceRequirements = bindings.getResourceRequirement(job);
            String resourceDirectives = getSlurmResourceRequirements(resourceRequirements);
            slurmCommand += resourceDirectives;
            logger.debug("Sending slurm job");

            String cwlJob = EncodingHelper.decodeBase64(job.getApp());
            cwlJob = stripLeadingJSONCharacters(cwlJob);
            File cwlJobFile = new File(workingDir, "job.json");
            FileUtils.writeStringToFile(cwlJobFile, cwlJob);

            File inputsFile = CWLJobInputsWriter.createInputsFile(job, workingDir);
//            String bunnyCLIPath = "java -jar /media/anton/ECFA959BFA95631E2/Programming/SevenBridges/bunny/rabix-cli/test-target/rabix-cli-1.0.0-rc5.jar --configuration-dir /media/anton/ECFA959BFA95631E2/Programming/SevenBridges/bunny/rabix-cli/config2";
            // TODO: add option for specifying shared file storage location
            String bunnyCLIPath = "java -jar /vagrant/rabix-cli/test-target/rabix-cli-1.0.0-rc5.jar --configuration-dir /vagrant/rabix-cli/config2";
            String command = bunnyCLIPath + " " + cwlJobFile.getAbsolutePath() + " " + inputsFile.getAbsolutePath();
//            command = command.replace("media/anton/ECFA959BFA95631E2/Programming/SevenBridges/bunny/examples/", "vagrant/");
            slurmCommand += " --wrap=\"" + command + "\"";
            String s;
            logger.debug("Submitting command: " + slurmCommand);
            File commandFile = new File("command.sh");
            FileUtils.writeStringToFile(commandFile, slurmCommand);
            String[] commands = {"bash","command.sh"};
            ProcessBuilder pb = new ProcessBuilder(commands);
            pb.redirectErrorStream(true);
            Process p = pb.start();

            // Mock command
//             String command = "echo Submitted batch job 16";
//            Process proc = rt.exec(command);
//            Process proc = rt.exec(slurmCommand);
//            BufferedReader stdError = new BufferedReader(new
//                    InputStreamReader(proc.getErrorStream()));
//            Thread.sleep(1500);

            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(p.getInputStream()));
            logger.debug("input stream obtained");
            while ((s = stdInput.readLine()) != null) {
                logger.debug("String: " + s);

                if (s.startsWith("Submitted")) {
                    // Example output (in case of success): "Submitted batch job 16"
                    // TODO: handle errors
                    String pattern = "job\\s*\\d*";
                    Pattern r = Pattern.compile(pattern);
                    Matcher m = r.matcher(s);
                    if (m.find()) {
                        jobId = m.group(0).split("\\s")[1];
                        logger.debug("Submitted job " + jobId);
                    }else {
                        logger.debug("Submission went unsuccessfully");
                    }
                    output += s;
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
        String directive = "";
        if (requirements != null) {
            Long cpuMin = requirements.getCpuMin();
            Long memMin = requirements.getMemMinMB();
            if (cpuMin != null) {
                directive += " --ntasks-per-node=" + Long.toString(cpuMin);
            }
            if (memMin != null) {
//                directive += " --mem=" + Long.toString(memMin);
            }
        }
        return directive;
    }

    /**
     * method for replacing a regexp pattern in a string
     */
    public static String regexpReplacer(String source, String pattern, String replacer){
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(source);
        if (m.find( )) {
            source = source.replace(m.group(0), replacer);
        }
        return source;
    }

    /**
     * replaces garbage symbols at the beginning of base64 decoded app
     * @param jsonSource     decoded app
     * @return input string with stripped garbage symbols
     */
    private static String stripLeadingJSONCharacters(String jsonSource){
        String pattern = "^.+(?=\\{)";
        return regexpReplacer(jsonSource, pattern, "");
    }

}
