package kafkaadmin;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AclBinding;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

class kafkaadmin {

    public static void main(String[] args) {
        String configFilepath = "", propsFilepath = "";
        boolean executeFlag = false, readFlag = false, internalFlag = false;
        PrintStream configFile = System.out;

        CommandLine commandLine;
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption("c","config",true,"Config File Location");
        options.addRequiredOption("p","properties",true,"Connection Properties File Location");
        options.addOption("execute",false,"Execute Flag");
        options.addOption("dump",false,"Dump Current Topics/ACLs Server Configuration");
        options.addOption("internal",false,"Force Internal Topics Configuration");
        options.addOption("o", "output",true,"Output File Name For Config Dump");
        try {
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption("properties")) {
                propsFilepath = commandLine.getOptionValue("properties");
            }
            if (commandLine.hasOption("config")) {
                configFilepath = commandLine.getOptionValue("config");
            }
            if (commandLine.hasOption("internal")) {
                internalFlag = true;
            }
            if (commandLine.hasOption("execute")) {
                if (!commandLine.hasOption("config")) {
                    throw(new ParseException("Missing -config (-c) option for execute operation"));
                }
                executeFlag = true;
            } else {
                // only if not executing
                if (commandLine.hasOption("dump")) {
                    if (commandLine.hasOption("config")) {
                        throw(new ParseException("Cannot use -config (-c) option for the dump operation"));
                    }
                    readFlag = true;
                    if (commandLine.hasOption("output"))
                        configFile = new PrintStream(commandLine.getOptionValue("output"));
                }
            }
        }
        catch (ParseException exception)
        {
            System.err.println("Argument Error: " + exception.getMessage());
            if (exception.getMessage().equals("Missing required option: p")){
                System.err.println("You must specify a connection properties file location using the -p or -properties argument.");
            }
            System.exit(1);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.err.println("Error opening output file.");
            System.exit(1);
        }

        // read connection properties
        Properties props = new Properties();
        try (InputStream inProps = new FileInputStream(propsFilepath)) {
            props.load(inProps);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // Create our AdminClient using properties from our config file
        AdminClient client = AdminClient.create(props);

        if (readFlag){
            // only read and dump current config and exit
            System.err.println("Existing topics...");
            HashMap<String, HashMap<String, Object>> allTopics = topic.getTopics(client, internalFlag);
            if (allTopics != null)
              configFile.println(topic.topicsToYAML(allTopics));
            System.err.println("Existing acls...");
            Collection<AclBinding> aclInfo = acl.getAcls(client);
            if (aclInfo != null)
              configFile.println(acl.aclsToYAML(aclInfo));
            System.exit(0);
        }
        // First we need to read our yaml config
        JsonNode config = configloader.readConfig(configFilepath);

        //prepare topic lists & print topic plan here
        HashMap<String, Set<String>> topicLists = topic.prepareTopics(client,config);
        System.out.println("\n----- Topic Plan -----");
        for ( String key : topicLists.keySet()){
            System.out.println("\n" + key + ":");
            for (String value : topicLists.get(key)){
                System.out.println(value);
            }
        }

        if (executeFlag) {
            //create,modify, & delete the topics according to the plan
            System.out.print("\nCreating topics...");
            topic.createTopics(client, config, topicLists.get("createTopicList"));
            System.out.println("Done!");
            System.out.print("\nIncreasing partitions...");
            topic.increasePartitions(client, config, topicLists.get("increasePartitionList"));
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
        for ( String key : aclLists.keySet()){
            System.out.println("\n" + key + ":");
            for (AclBinding value : aclLists.get(key)){
                System.out.println(value);
            }
        }

        //create & delete the acls according to the plan
        if (executeFlag) {
            System.out.print("\nDeleting ACLs...");
            acl.deleteAcls(client, aclLists.get("deleteAclList"));
            System.out.println("Done!");

            System.out.print("\nCreating ACLs...");
            acl.createAcls(client, aclLists.get("createAclList"));
            System.out.println("Done!");
        }
        else {
            System.out.println("Skipping create & delete ACLs...use \"-execute\" to create or delete ACLs from the plan.");
        }
        System.out.println("----------------------");
    }
}
