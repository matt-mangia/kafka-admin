package kafkaadmin.bindingclient;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafkaadmin.model.AclRule;
import kafkaadmin.model.CentralizedAclBinding;
import kafkaadmin.model.RoleBinding;
import kafkaadmin.model.RoleBindingResource;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BindingLoader {
    static Map<String, Collection> cacheMap = null;
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = RoleBindingKey.class, name = "RoleBinding"),
            @JsonSubTypes.Type(value = AclBindingKey.class, name = "AclBinding")
    })
    interface AuthJSONKey {
    }
    static class RoleBindingKey implements AuthJSONKey {
        public String principal;
        public String role;
        public Map<String, Map<String,String>> scope;
        public String toString() {
            return new com.google.gson.Gson().toJson(this);
        }
    }
    static class AclBindingKey implements AuthJSONKey {
        public Map<String,String> resourcePattern;
        public Map<String,Map<String,String>> scope;
        public String toString() {
            return new com.google.gson.Gson().toJson(this);
        }
    }
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = RoleBindingValue.class, name = "RoleBinding"),
            @JsonSubTypes.Type(value = AclBindingValue.class, name = "AclBinding")
    })
    interface AuthJSONValue {

    }
    static class RoleBindingValue implements AuthJSONValue {
        public ArrayList<RoleBindingResource> resources;
        public String toString() {
            return new com.google.gson.Gson().toJson(this);
        }
    }
    static class AclBindingValue implements AuthJSONValue {
        public ArrayList<AclRule> aclRules;
        public String toString() {
            return new com.google.gson.Gson().toJson(this);
        }
    }

    // plain old consumer, read RoleBinding messages and compose a final list of RoleBindings
    public static Map<String, Collection> getRolebindingsAndAcls(Properties props) {
        // try caching
        if (cacheMap != null)
            return cacheMap;
        final String authTopic = props.getProperty("auth.topic.name", "_confluent-metadata-auth");
        final Long pollTimeout = Long.parseLong(props.getProperty("poll.timeout.ms","10000"));
        final Map<String, String> consumerBindingMap = new HashMap<String, String>();
        final Map<String, String> roleBindingMap = new HashMap<String, String>();
        final Map<String, String> aclBindingMap = new HashMap<String, String>();
        final Map<String, Collection> resultMap = new HashMap<>();
        final ObjectMapper om = new ObjectMapper();
        // from beginning and no commit
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        final Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(authTopic));
        // extract type to avoid storing everything
        Pattern pattern = Pattern.compile("^\\{\\\"_type\\\":\\\"(\\w+)\\\".*");
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));
                if (records.isEmpty()) {
                    // log end
                    break;
                }
                for (ConsumerRecord<String, String> record : records) {
                    Matcher matcher = pattern.matcher(record.key());
                    if (matcher.matches() && matcher.group(1).equals("RoleBinding") || matcher.group(1).equals("AclBinding")) {
                        if (record.value() != null) {
                            consumerBindingMap.put(record.key(), record.value());
                        } else {
                            // tombstone
                            consumerBindingMap.remove(record.key());
                        }
                    }
                }
            }
        } finally {
            consumer.close();
        }
        // map rolebindings and aclbindings
        ArrayList<RoleBinding> returnListRoleBinding = new ArrayList<RoleBinding>();
        ArrayList<CentralizedAclBinding> returnListCentralizedAclBinding = new ArrayList<CentralizedAclBinding>();
        for (Map.Entry<String, String> tempBinds : consumerBindingMap.entrySet()) {
            try {
                Matcher matcher = pattern.matcher(tempBinds.getKey());
                matcher.matches();
                switch (matcher.group(1)) {
                    case "RoleBinding":
                        RoleBindingKey rbKey = om.readValue(tempBinds.getKey(), RoleBindingKey.class);
                        RoleBindingValue rbValue = om.readValue(tempBinds.getValue(), RoleBindingValue.class);
                        RoleBinding rb = new RoleBinding();
                        rb.principal =  rbKey.principal;
                        rb.role = rbKey.role;
                        rb.scope = rbKey.scope;
                        rb.resources = rbValue.resources;
                        returnListRoleBinding.add(rb);
                        break;
                    case "AclBinding":
                        AclBindingKey abKey = om.readValue(tempBinds.getKey(), AclBindingKey.class);
                        AclBindingValue abValue = om.readValue(tempBinds.getValue(), AclBindingValue.class);
                        CentralizedAclBinding ab = new CentralizedAclBinding();
                        ab.scope =  abKey.scope;
                        ab.resourcePattern = abKey.resourcePattern;
                        ab.aclRules = abValue.aclRules;
                        returnListCentralizedAclBinding.add(ab);
                        break;
                }
            } catch (JsonMappingException e) {
                e.printStackTrace();
                // expected as there are other message types in the topic
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
        resultMap.put("RoleBinding", returnListRoleBinding);
        resultMap.put("AclBinding", returnListCentralizedAclBinding);
        cacheMap = resultMap;
        return resultMap;
    }
}
