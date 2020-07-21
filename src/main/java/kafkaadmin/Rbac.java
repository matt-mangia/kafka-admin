package kafkaadmin;
// Import classes:
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import kafkaadmin.mdsclient.MDSClient;
import kafkaadmin.model.RoleBinding;
import kafkaadmin.model.RoleBindingResource;
import org.apache.kafka.clients.consumer.*;

import java.util.*;

import java.time.Duration;
import java.util.Properties;

class Rbac {
  public static void pushRoleBindings(MDSClient client, Collection<RoleBinding> newRoleBindings, Properties props, String token, Boolean delete)
  {
    if (newRoleBindings == null) {
      return;
    }
    for (RoleBinding roleBinding : newRoleBindings) {
      if (roleBinding.resources == null) {
        if (!delete)
          client.addRoleForPrincipal(roleBinding.principal, roleBinding.role, roleBinding.scope);
        else
          client.deleteRoleForPrincipal(roleBinding.principal, roleBinding.role, roleBinding.scope);
      } else {
        if (!delete)
          client.addRoleResourcesForPrincipal(roleBinding.principal, roleBinding.role, roleBinding.scope, roleBinding.resources);
        else
          client.deleteRoleResourcesForPrincipal(roleBinding.principal, roleBinding.role, roleBinding.scope, roleBinding.resources);
      }
    }
  }
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_type")
  @JsonSubTypes({
          @JsonSubTypes.Type(value = RoleBindingKey.class, name = "RoleBinding")
  })
  public interface AuthJSONKey {

  }
  static class RoleBindingKey implements AuthJSONKey {
    public String principal;
    public String role;
    public Map<String,Map<String,String>> scope;
    public String toString() {
      return new com.google.gson.Gson().toJson(this);
    }
  }
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_type")
  @JsonSubTypes({
          @JsonSubTypes.Type(value = RoleBindingValue.class, name = "RoleBinding")
  })
  public interface AuthJSONValue {

  }
  static class RoleBindingValue implements AuthJSONValue {
    public ArrayList<RoleBindingResource> resources;
    public String toString() {
      return new com.google.gson.Gson().toJson(this);
    }
  }

  // plain old consumer, read RoleBinding messages and compose a final list of RoleBindings
  public static ArrayList<RoleBinding> getRolebindings(Properties props) {
    final String authTopic = props.getProperty("auth.topic.name");
    final Map<String, String> roleBindingMap = new HashMap<String, String>();
    final ObjectMapper om = new ObjectMapper();
    // from beginning and no commit
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    final Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
    consumer.subscribe(Arrays.asList(authTopic));
    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        if (records.isEmpty()) {
          // log end
          break;
        }
        for (ConsumerRecord<String, String> record : records) {
          if (record.value() != null) {
            roleBindingMap.put(record.key(), record.value());
          } else {
            // tombstone
            roleBindingMap.remove(record.key());
          }
        }
      }
    } finally {
      consumer.close();
    }
    // map rolebindings
    ArrayList<RoleBinding> returnList = new ArrayList<RoleBinding>();
    for (Map.Entry<String, String> roleBinds : roleBindingMap.entrySet()) {
      RoleBindingKey rbKey = null;
      RoleBindingValue rbValue = null;
      try {
        rbKey = om.readValue(roleBinds.getKey(), RoleBindingKey.class);
        rbValue = om.readValue(roleBinds.getValue(), RoleBindingValue.class);
        // compose resulting list of RoleBindings
        RoleBinding rb = new RoleBinding();
        rb.principal = rbKey.principal;
        rb.role = rbKey.role;
        rb.scope = rbKey.scope;
        rb.resources = rbValue.resources;
        returnList.add(rb);
      } catch (JsonMappingException e) {
        // e.printStackTrace();
        // expected as there are other message types in the topic
      } catch (JsonProcessingException e) {
        // e.printStackTrace();
      }
    }
    return returnList;
  }
  public static String roleBindingsToYAML(Collection<RoleBinding> currentRoleBindings) {
    //Get current RoleBindings into a map then format as YAML
    HashMap<String, HashMap<String, HashMap<String,Object>>> rolebindMap = new HashMap<>();
    LinkedHashMap<String, HashMap<String,Object>> rolebindItem = new LinkedHashMap<>();
    int counter = 1;
    for(RoleBinding roleBinding : currentRoleBindings) {
      HashMap<String, Object> rolebindingConfig = new HashMap<>();
      rolebindingConfig.put("principal", roleBinding.principal);
      rolebindingConfig.put("role", roleBinding.role);
      rolebindingConfig.put("scope",roleBinding.scope);
      if (roleBinding.resources != null)
        rolebindingConfig.put("resource",roleBinding.resources);
      rolebindItem.put("RoleBinding-"+counter++,rolebindingConfig);
    }
    rolebindMap.put("rolebindings",rolebindItem);
    YAMLFactory yFact = new YAMLFactory();
    yFact.configure(YAMLGenerator.Feature.MINIMIZE_QUOTES,true);
    yFact.configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER,false);
    ObjectMapper mapper = new ObjectMapper(yFact);
    String yamlString = null;
    try {
      yamlString = mapper.writeValueAsString(rolebindMap);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      // TODO: some handling
    }
    return yamlString;
  }

  public static HashMap<String, Collection<RoleBinding>> prepareRoleBindings(Properties props, JsonNode config) {
    if (!config.hasNonNull("rolebindings")) {
      return null;
    }
    //Get current RoleBindings
    Collection<RoleBinding> currentRoleTemp = getRolebindings(props);
    // expand the resourcePatterns for easy delta evaluation
    Collection<RoleBinding> currentRoleBindings = new ArrayList<>();
    for (RoleBinding roleBinding : currentRoleTemp) {
      if (roleBinding.resources != null) {
        for (RoleBindingResource resource : roleBinding.resources) {
          RoleBinding rbTemp = new RoleBinding(roleBinding);
          rbTemp.resources = new ArrayList<>(Arrays.asList(resource));
          currentRoleBindings.add(rbTemp);
        }
      } else {
        currentRoleBindings.add(roleBinding);
      }
    }
    //Get configured RoleBindings
    Collection<RoleBinding> configuredRoleBindings = new ArrayList<>();
    ObjectMapper mapper = new ObjectMapper();
    for (JsonNode rbac : config.get("rolebindings")) {
      RoleBinding rbTemp = new RoleBinding();
      rbTemp.principal = rbac.get("principal").textValue();
      rbTemp.role = rbac.get("role").textValue();
      rbTemp.scope = mapper.convertValue(rbac.get("scope"), new TypeReference<Map<String, Map<String,String>>>(){});
      if (rbac.has("resource")) {
        Collection<RoleBindingResource> resources = mapper.convertValue(rbac.get("resource"), new TypeReference<ArrayList<RoleBindingResource>>(){});
        for (RoleBindingResource resource : resources) {
          rbTemp.resources = new ArrayList<>(Arrays.asList(resource));
          configuredRoleBindings.add(rbTemp);
          rbTemp = new RoleBinding(rbTemp);
        }
      } else {
        configuredRoleBindings.add(rbTemp);
      }
    }

    //Determine RoleBindings to remove
    Collection<RoleBinding> removeRoleBindings = new ArrayList<>(currentRoleBindings);
    removeRoleBindings.removeAll(configuredRoleBindings);

    //Determine RoleBindings to add
    Collection<RoleBinding> addRoleBindings = new ArrayList<>(configuredRoleBindings);
    addRoleBindings.removeAll(currentRoleBindings);

    HashMap<String,Collection<RoleBinding>> roleBindingsPlan = new HashMap<>();
    roleBindingsPlan.put("createRoleBindingsList", addRoleBindings);
    roleBindingsPlan.put("deleteRoleBindingsList", removeRoleBindings);

    return roleBindingsPlan;
  }
}
