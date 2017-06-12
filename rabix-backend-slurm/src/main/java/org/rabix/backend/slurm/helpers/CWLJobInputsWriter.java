package org.rabix.backend.slurm.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.rabix.backend.tes.service.impl.LocalTESStorageServiceImpl;
import org.rabix.bindings.model.FileValue;
import org.rabix.bindings.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.util.Map;


public class CWLJobInputsWriter {
    private final static Logger logger = LoggerFactory.getLogger(LocalTESStorageServiceImpl.class);

    /**
     * creates CWL inputs.json inside @baseDir from @job's inputs
     * TODO: extend for types other than File
     */
    public static File createInputsFile(Job job, File baseDir){
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.createObjectNode();
        Map<String, Object> inputs = job.getInputs();
        for (Map.Entry<String, Object> input: inputs.entrySet()){
            JsonNode childNode2 = mapper.createObjectNode();
            Object objectValue = input.getValue();
            if (objectValue instanceof FileValue) {
                FileValue value = (FileValue) objectValue;
                ((ObjectNode) childNode2).put("class", "File");
                ((ObjectNode) childNode2).put("path", value.getPath());
                ((ObjectNode) rootNode).set(input.getKey(), childNode2);
            } else if (objectValue instanceof Boolean){

            }else{
                throw new NotImplementedException();
            }
        }
        File inputsFile = new File(baseDir, "inputs.json");
        try {
            mapper.writeValue(inputsFile, rootNode);
        }catch (IOException e){
            e.printStackTrace();
        }
        return inputsFile;
    }
}
