package kafkaadmin;

import com.fasterxml.jackson.databind.JsonNode;
import kafkaadmin.mdsclient.MDSClient;
import kafkaadmin.model.RoleBinding;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AclBinding;

import java.io.*;
import java.util.*;

class KafkaAdmin {

    public static void main(String[] args) {
        String configFilepath = "", propsFilepath = "";
        boolean executeFlag = false, dumpFlag = false, internalFlag = false, enableDelete = false, rbacProcess = false;
        PrintStream configFile = System.out;
        String rbacToken = "";

        CommandLine commandLine;
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption("c","config",true,"Config File Location");
        options.addRequiredOption("p","properties",true,"Connection Properties File Location");
        options.addOption("execute",false,"Execute Flag");
        options.addOption("dump",false,"Dump Current Topics/ACLs Server Configuration");
        options.addOption("internal",false,"Force Internal Topics Configuration");
        options.addOption("o", "output",true,"Output File Name For Config Dump");
        options.addOption("d", "delete",false,"Enable delete topic(s) operations");
        options.addOption("r", "rbac",false,"Test RBAC operations");
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
            if (commandLine.hasOption("delete")) {
                enableDelete = true;
            }
            if (commandLine.hasOption("rbac")) {
                rbacProcess = true;
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
                    dumpFlag = true;
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
        MDSClient mdsClient = null;
        if (rbacProcess) {
            mdsClient = new MDSClient();
            rbacToken = mdsClient.authenticate(props.getProperty("mds.url"),props.getProperty("mds.user"),props.getProperty("mds.password"));
            if (rbacToken == null) {
                System.err.println("Unable to login to MDS with the provided credentials.");
                System.exit(1);
            }
        }

        if (dumpFlag){
            // only read and dump current config and exit
            System.err.println("Existing topics...");
            HashMap<String, HashMap<String, Object>> allTopics = Topic.getTopics(client, internalFlag);
            if (allTopics != null)
              configFile.println(Topic.topicsToYAML(allTopics));
            System.err.println("Existing acls...");
            Collection<AclBinding> aclInfo = Acl.getAcls(client);
            if (aclInfo != null)
              configFile.println(Acl.aclsToYAML(aclInfo));
            if (rbacProcess) {
                System.err.println("Existing RoleBindings...");
                Collection<RoleBinding> roleBindingInfo = Rbac.getRolebindings(props);
                if (roleBindingInfo != null)
                    configFile.println(Rbac.roleBindingsToYAML(roleBindingInfo));
            }
            System.exit(0);
        }
        // First we need to read our yaml config
        JsonNode config = ConfigLoader.readConfig(configFilepath);
        //prepare topic lists & print topic plan here
        HashMap<String, Set<String>> topicLists = Topic.prepareTopics(client, config);
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
            Topic.createTopics(client, config, topicLists.get("createTopicList"));
            System.out.println("Done!");
            System.out.print("\nIncreasing partitions...");
            Topic.increasePartitions(client, config, topicLists.get("increasePartitionList"));
            System.out.println("Done!");
            if (enableDelete) {
                System.out.print("\nDeleting topics...");
                Topic.deleteTopics(client, topicLists.get("deleteTopicList"));
                System.out.println("Done!");
            } else {
                System.out.println("Skipping topics delete ...use \"-delete\" to execute the deletion.");
            }
        }
        else {
            System.out.println("Skipping topics management...use \"-execute\" to apply the plan.");
        }
        System.out.println("----------------------");

        //prepare Acl lists & print Acl plan here
        HashMap<String, Collection<AclBinding>> aclLists = Acl.prepareAcls(client,config);
        System.out.println("\n----- ACL Plan -----");
        if (aclLists != null)
            for ( String key : aclLists.keySet()){
                System.out.println("\n" + key + ":");
                for (AclBinding value : aclLists.get(key)){
                    System.out.println(value);
                }
            }

        //create & delete the acls according to the plan
        if (executeFlag) {
            // check for systems without authorizer
            if (aclLists != null) {
                System.out.print("\nDeleting ACLs...");
                Acl.deleteAcls(client, aclLists.get("deleteAclList"));
                System.out.println("Done!");

                System.out.print("\nCreating ACLs...");
                Acl.createAcls(client, aclLists.get("createAclList"));
                System.out.println("Done!");
            }
        }
        else {
            System.out.println("Skipping create & delete ACLs...use \"-execute\" to create or delete ACLs from the plan.");
        }
        //prepare RoleBinding lists & print RoleBinding plan here
        HashMap<String, Collection<RoleBinding>> roleBindingLists = Rbac.prepareRoleBindings(props,config);
        System.out.println("\n----- RoleBinding Plan -----");
        if (roleBindingLists != null)
            for ( String key : roleBindingLists.keySet()){
                System.out.println("\n" + key + ":");
                for (RoleBinding value : roleBindingLists.get(key)){
                    System.out.println(value);
                }
            }

        //create & delete the RoleBindings according to the plan
        if (executeFlag) {
            // check for systems without authorizer
            if (roleBindingLists != null) {
                System.out.print("\nDeleting RoleBindings...");
                Rbac.pushRoleBindings(mdsClient, roleBindingLists.get("deleteRoleBindingsList"),props, rbacToken, Boolean.TRUE);
                System.out.println("Done!");

                System.out.print("\nCreating RoleBindings...");
                Rbac.pushRoleBindings(mdsClient, roleBindingLists.get("createRoleBindingsList"),props, rbacToken, Boolean.FALSE);
                System.out.println("Done!");
            }
        }
        else {
            System.out.println("Skipping create & delete RoleBindings...use \"-execute\" to create or delete RoleBindings from the plan.");
        }
        System.out.println("----------------------");
    }
}
