package kafkaadmin;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.commons.cli.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

class kafkaadmin {

    public static void main(String[] args) {
        String configFilepath = "";
        boolean executeFlag = false;

        CommandLine commandLine;
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addRequiredOption("c","config",true,"Config File Location");
        options.addOption("execute",false,"Execute Flag");

        try {
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption("config")) {
                configFilepath = commandLine.getOptionValue("config");
            }
            if (commandLine.hasOption("execute")) {
                executeFlag = true;
            }
        }
        catch (ParseException exception)
        {
            System.out.println("Argument Error: " + exception.getMessage());
            if (exception.getMessage().equals("Missing required option: c")){
                System.out.println("You must specify a config file location using the -c or -config argument.");
            }
            System.exit(1);
        }

        // First we need to read our yaml config
        JsonNode config = configloader.readConfig(configFilepath);

        // Create our AdminClient using properties from our config file
        AdminClient client = AdminClient.create(configloader.createProps(config));

        //prepare topic lists & print topic plan here
        HashMap<String, Set<String>> topicLists = topic.prepareTopics(client,config);
        System.out.println("\n----- Topic Plan -----");
        System.out.println(topicLists + "\n");

        if (executeFlag) {
            //create & delete the topics according to the plan
            System.out.print("Creating topics...");
            topic.createTopics(client, config, topicLists.get("createTopicList"));
            System.out.println("Done!");
        }
        else {
            System.out.println("Skipping create topics...use \"-execute\" to create topics from the plan.");
        }
        System.out.println("----------------------");


        //Commenting out deletion of topics -- to be done manually
        //topic.deleteTopics(client,topicLists.get("deleteTopicList"));

        //prepare acl lists & print acl plan here
        HashMap<String, Collection<AclBinding>> aclLists = acl.prepareAcls(client,config);
        System.out.println("\n----- ACL Plan -----");
        System.out.println(aclLists + "\n");

        //create & delete the acls according to the plan
        if (executeFlag) {
            System.out.print("Deleting ACLs...");
            acl.deleteAcls(client, aclLists.get("deleteAclList"));
            System.out.println("Done!");

            System.out.print("Creating ACLs...");
            acl.createAcls(client, aclLists.get("createAclList"));
            System.out.println("Done!");
        }
        else {
            System.out.println("Skipping create & delete ACLs...use \"-execute\" to create or delete ACLs from the plan.");
        }
        System.out.println("----------------------");
    }
}
