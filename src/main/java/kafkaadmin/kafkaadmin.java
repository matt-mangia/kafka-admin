package kafkaadmin;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AclBinding;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

class kafkaadmin {

    public static void main(String[] args) {
        String configFilepath = "";
        try {
            configFilepath = args[0];
        }
        catch (ArrayIndexOutOfBoundsException e){
            System.out.println("You must specify a config filepath!");
            System.exit(1);
        }

        // First we need to read our yaml config
        JsonNode config = configloader.readConfig(configFilepath);

        // Create our AdminClient using properties from our config file
        AdminClient client = AdminClient.create(configloader.createProps(config));

        //prepare topic lists & print topic plan here
        HashMap<String, Set<String>> topicLists = topic.prepareTopics(client,config);
        System.out.println(topicLists);

        //create & delete the topics according to the plan
        topic.createTopics(client,config,topicLists.get("createTopicList"));

        //Commenting out deletion of topics -- to be done manually
        //topic.deleteTopics(client,topicLists.get("deleteTopicList"));

        //prepare acl lists & print acl plan here
        HashMap<String, Collection<AclBinding>> aclLists = acl.prepareAcls(client,config);
        System.out.println(aclLists);

        //create & delete the acls according to the plan
        acl.deleteAcls(client,aclLists.get("deleteAclList"));
        acl.createAcls(client,aclLists.get("createAclList"));
    }
}
