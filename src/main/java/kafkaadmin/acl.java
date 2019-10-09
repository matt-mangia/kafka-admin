package kafkaadmin;

import com.fasterxml.jackson.databind.JsonNode;
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
            //Get current ACLs
            Collection<AclBinding> currentAcls = client.describeAcls(AclBindingFilter.ANY).values().get();

            //Get configured ACLs
            Collection<AclBinding> configuredAcls = new ArrayList<>();

            for (JsonNode acl : config.get("acls")) {
                ResourceType resType = ResourceType.fromString(acl.get("resource-type").textValue());
                PatternType patType = PatternType.fromString(acl.get("resource-pattern").textValue());
                ResourcePattern resourcePattern = new ResourcePattern(resType,acl.get("resource-name").textValue(),patType);

                AclOperation aclOp = AclOperation.fromString(acl.get("operation").textValue());
                AclPermissionType aclPerm = AclPermissionType.fromString(acl.get("permission").textValue());

                AccessControlEntry accessControlEntry = new AccessControlEntry(acl.get("principal").textValue(), acl.get("host").textValue(), aclOp, aclPerm);

                AclBinding aclBinding = new AclBinding(resourcePattern,accessControlEntry);
                configuredAcls.add(aclBinding);
            }

            //Determine acls to remove
            Collection<AclBinding> removeAcls = new ArrayList<>(currentAcls);
            removeAcls.removeAll(configuredAcls);

            //Determine acls to add
            Collection<AclBinding> addAcls = new ArrayList<>(configuredAcls);
            addAcls.removeAll(currentAcls);

            HashMap<String,Collection<AclBinding>> aclPlan = new HashMap<>();
            aclPlan.put("createAclList", addAcls);
            aclPlan.put("deleteAclList", removeAcls);

            return aclPlan;

        } catch (InterruptedException | ExecutionException e) {
            System.out.println(e);
            return null;
        }
    }
}
