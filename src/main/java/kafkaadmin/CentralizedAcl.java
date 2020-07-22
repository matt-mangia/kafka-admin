package kafkaadmin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import kafkaadmin.mdsclient.MDSClient;
import kafkaadmin.model.AclRule;
import kafkaadmin.model.CentralizedAclBinding;

import java.util.*;

import static kafkaadmin.bindingclient.BindingLoader.getRolebindingsAndAcls;

class CentralizedAcl {
    public static void pushAclBindings(MDSClient client, Collection<CentralizedAclBinding> newAclBindings,
                                       Properties props, String token, Boolean delete) {
        if (newAclBindings == null) {
            return;
        }
        for (CentralizedAclBinding aclBinding : newAclBindings) {
            if (!delete)
                client.addCentralizedAcls(aclBinding.resourcePattern, aclBinding.scope, aclBinding.aclRules);
            else
                client.deleteCentralizedAcls(aclBinding.resourcePattern, aclBinding.scope, aclBinding.aclRules);
        }
    }

    public static String aclBindingsToYAML(Collection<CentralizedAclBinding> currentCentralizedAclBindings) {
        //Get current RoleBindings into a map then format as YAML
        HashMap<String, HashMap<String, HashMap<String, Object>>> aclBindMap = new HashMap<>();
        LinkedHashMap<String, HashMap<String, Object>> aclBindItem = new LinkedHashMap<>();
        int counter = 1;
        for (CentralizedAclBinding centralizedAclBinding : currentCentralizedAclBindings) {
            HashMap<String, Object> aclBindingConfig = new HashMap<>();
            aclBindingConfig.put("resourcePattern", centralizedAclBinding.resourcePattern);
            aclBindingConfig.put("scope", centralizedAclBinding.scope);
            if (centralizedAclBinding.aclRules != null)
                aclBindingConfig.put("aclRules", centralizedAclBinding.aclRules);
            aclBindItem.put("AclBinding-" + counter++, aclBindingConfig);
        }
        aclBindMap.put("aclbindings", aclBindItem);
        YAMLFactory yFact = new YAMLFactory();
        yFact.configure(YAMLGenerator.Feature.MINIMIZE_QUOTES, true);
        yFact.configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false);
        ObjectMapper mapper = new ObjectMapper(yFact);
        String yamlString = null;
        try {
            yamlString = mapper.writeValueAsString(aclBindMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            // TODO: some handling
        }
        return yamlString;
    }

    public static HashMap<String, Collection<CentralizedAclBinding>> prepareAclBindings(Properties props, JsonNode config) {
        if (!config.hasNonNull("aclbindings")) {
            return null;
        }
        //Get current AclBindings
        // TODO - optimize to avoid reloading when both RB and AB is done
        Collection<CentralizedAclBinding> currentAclTemp = getRolebindingsAndAcls(props).get("AclBinding");
        // expand the resourcePatterns for easy delta evaluation
        Collection<CentralizedAclBinding> currentAclBindings = new ArrayList<>();
        for (CentralizedAclBinding aclBinding : currentAclTemp) {
            for (AclRule aclRule : aclBinding.aclRules) {
                CentralizedAclBinding abTemp = new CentralizedAclBinding(aclBinding);
                abTemp.aclRules = new ArrayList<>(Arrays.asList(aclRule));
                currentAclBindings.add(abTemp);
            }
        }
        //Get configured AclBindings
        Collection<CentralizedAclBinding> configuredAclBindings = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        for (JsonNode cAcl : config.get("aclbindings")) {
            CentralizedAclBinding abTemp = new CentralizedAclBinding();
            abTemp.resourcePattern = mapper.convertValue(cAcl.get("resourcePattern"), new TypeReference<Map<String, String>>() {
            });
            abTemp.scope = mapper.convertValue(cAcl.get("scope"), new TypeReference<Map<String, Map<String, String>>>() {
            });
            Collection<AclRule> aclRules = mapper.convertValue(cAcl.get("aclRules"), new TypeReference<ArrayList<AclRule>>() {
                });
            for (AclRule cAclRule : aclRules) {
                abTemp.aclRules = new ArrayList<>(Arrays.asList(cAclRule));
                configuredAclBindings.add(abTemp);
                abTemp = new CentralizedAclBinding(abTemp);
            }
        }

        //Determine AclBindings to remove
        Collection<CentralizedAclBinding> removeAclBindings = new ArrayList<>(currentAclBindings);
        removeAclBindings.removeAll(configuredAclBindings);

        //Determine AclBindings to add
        Collection<CentralizedAclBinding> addAclBindings = new ArrayList<>(configuredAclBindings);
        addAclBindings.removeAll(currentAclBindings);

        HashMap<String, Collection<CentralizedAclBinding>> aclBindingsPlan = new HashMap<>();
        aclBindingsPlan.put("createAclBindingsList", addAclBindings);
        aclBindingsPlan.put("deleteAclBindingsList", removeAclBindings);

        return aclBindingsPlan;
    }
}
