package kafkaadmin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutionException;

class acl {

    public static void createAcls(AdminClient client, Collection<AclBinding> addAclList) {
        try {
            final CreateAclsResult createAclsResult = client.createAcls(addAclList);
            createAclsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println(e);
        }

    }

    public static void deleteAcls(AdminClient client, Collection<AclBinding> deleteAclList) {
        Collection<AclBindingFilter> filters = new ArrayList<>();
        for (AclBinding acl : deleteAclList){
            filters.add(acl.toFilter());
        }
        try {
            final DeleteAclsResult deleteAclsResult = client.deleteAcls(filters);
            deleteAclsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println(e);
        }
    }

    public static HashMap<String, Collection<AclBinding>> prepareAcls(AdminClient client, JsonNode config) {
        try {
            if (!config.hasNonNull("acls")) {
                return null;
            }
            //Get current ACLs
            Collection<AclBinding> currentAcls = client.describeAcls(AclBindingFilter.ANY).values().get();
            //Get configured ACLs
            Collection<AclBinding> configuredAcls = new ArrayList<>();

            for (JsonNode acl : config.get("acls")) {
                for (String principal : acl.get("principal").textValue().split(",")) {
                    for (String operation : acl.get("operation").textValue().split(",")) {
                        for (String resourcename : acl.get("resource-name").textValue().split(",")) {
                            ResourceType resType = ResourceType.fromString(acl.get("resource-type").textValue());
                            PatternType patType = PatternType.fromString(acl.get("resource-pattern").textValue());
                            ResourcePattern resourcePattern = new ResourcePattern(resType, resourcename.trim(), patType);

                            AclOperation aclOp = AclOperation.fromString(operation.trim());
                            AclPermissionType aclPerm = AclPermissionType.fromString(acl.get("permission").textValue());

                            AccessControlEntry accessControlEntry = new AccessControlEntry(principal.trim(), acl.get("host").textValue(), aclOp, aclPerm);

                            AclBinding aclBinding = new AclBinding(resourcePattern, accessControlEntry);
                            configuredAcls.add(aclBinding);
                        }
                    }
                }
            }

            //Determine acls to remove
            Collection<AclBinding> removeAcls = new ArrayList<>(currentAcls);
            removeAcls.removeAll(configuredAcls);

            //Determine acls to add
            Collection<AclBinding> addAcls = new ArrayList<>(configuredAcls);
            addAcls.removeAll(currentAcls);

            HashMap<String, Collection<AclBinding>> aclPlan = new HashMap<>();
            aclPlan.put("createAclList", addAcls);
            aclPlan.put("deleteAclList", removeAcls);

            return aclPlan;
        } catch (InterruptedException | ExecutionException e) {
            System.out.println(e);
            return null;
        }
    }
    public static Collection<AclBinding> getAcls(AdminClient client) {
        try {
            //Get current ACLs
            return client.describeAcls(AclBindingFilter.ANY).values().get();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println(e);
            return null;
        }
    }

    public static String aclsToYAML(Collection<AclBinding> currentAcls) {
        //Get current ACLs into a map then format as YAML
        HashMap<String, HashMap<String, HashMap<String,String>>> aclMap = new HashMap<>();
        LinkedHashMap<String, HashMap<String,String>> aclItem = new LinkedHashMap<>();
        int counter = 1;
        for(AclBinding k : currentAcls) {
            HashMap<String, String> aclConfig = new HashMap<>();
            aclConfig.put("resource-type", k.pattern().resourceType().name());
            aclConfig.put("resource-name", k.pattern().name());
            aclConfig.put("resource-pattern", k.pattern().patternType().name());
            aclConfig.put("principal", k.entry().principal());
            aclConfig.put("host", k.entry().host());
            aclConfig.put("operation", k.entry().operation().name());
            aclConfig.put("permission", k.entry().permissionType().name());
            aclItem.put("Acl-"+counter++,aclConfig);
        }
        aclMap.put("acls",aclItem);
        YAMLFactory yFact = new YAMLFactory();
        yFact.configure(YAMLGenerator.Feature.MINIMIZE_QUOTES,true);
        yFact.configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER,false);
        ObjectMapper mapper = new ObjectMapper(yFact);
        String yamlString = null;
        try {
            yamlString = mapper.writeValueAsString(aclMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            // TODO: some handling
        }
        return yamlString;
    }
}
