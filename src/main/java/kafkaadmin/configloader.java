package kafkaadmin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

class configloader {

    private static String readFile(String filepath) {
        try {
            File file = new File(filepath);
            return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        }
        catch(IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static JsonNode readConfig(String filepath) {
        Yaml yaml = new Yaml();
        LinkedHashMap<String, String> yamlconfig = yaml.load(readFile(filepath));

        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readTree(mapper.writeValueAsString(yamlconfig));
        }
        catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Properties createProps(JsonNode config){
        Properties properties = new Properties();
        Iterator<Map.Entry<String,JsonNode>> iterator = config.get("cluster").fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> prop = iterator.next();
            if (!prop.getValue().isNull()) {
                properties.put(prop.getKey(), prop.getValue().textValue());
            }
        }
        return properties;
    }

}

