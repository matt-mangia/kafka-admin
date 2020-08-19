package kafkaadmin;
// Import classes:
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import kafkaadmin.mdsclient.MDSClient;
import kafkaadmin.model.RoleBinding;
import kafkaadmin.model.RoleBindingResource;

import java.util.*;

import java.util.Properties;

import static kafkaadmin.bindingclient.BindingLoader.getRolebindingsAndAcls;

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
    Collection<RoleBinding> currentRoleTemp = getRolebindingsAndAcls(props).get("RoleBinding");
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
