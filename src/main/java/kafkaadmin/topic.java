package kafkaadmin;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

class topic {

    public static void printTopics(AdminClient client, boolean isInternal) {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(isInternal);
        try {
            for (TopicListing topic : client.listTopics(options).listings().get()) {
                System.out.println(topic.name());
            }
        } catch (InterruptedException | ExecutionException e) {
            System.out.println(e);
        }
    }

    private static HashMap<String,String> topicHeaderMap(JsonNode config) {
        HashMap<String, String> topicHeaderMap = new HashMap<>();
        JsonNode topicConfig = config.get("topics");
        Iterator<String> iter = topicConfig.fieldNames();
        while (iter.hasNext()) {
            String header = iter.next();
            topicHeaderMap.put(topicConfig.get(header).get("name").textValue(), header);
        }
        return topicHeaderMap;
    }

    public static void createTopics(AdminClient client, JsonNode config, Set<String> topicList) {
        Collection<NewTopic> newTopicList = new ArrayList<>();

        HashMap<String,String> topicHeaderMap = topicHeaderMap(config);
        JsonNode topicConfigs = config.get("topics");

        for (String t : topicList) {
            String header = topicHeaderMap.get(t);
            JsonNode topicConfig = topicConfigs.get(header);
            NewTopic newTopic = new NewTopic(topicConfig.get("name").asText(), topicConfig.get("partitions").asInt(), topicConfig.get("replication.factor").shortValue());

            Map<String,String> customConfigs = new HashMap<>();
            Iterator<Map.Entry<String,JsonNode>> iterator = topicConfig.fields();
            while (iterator.hasNext()) {
                Map.Entry<String, JsonNode> configs = iterator.next();
                if (!configs.getValue().isNull() && !configs.getKey().equals("name") && !configs.getKey().equals("partitions") && !configs.getKey().equals("replication.factor")) {
                    customConfigs.put(configs.getKey(), configs.getValue().textValue());
                }
            }
            newTopic.configs(customConfigs);
            newTopicList.add(newTopic);
        }

        try {
            final CreateTopicsResult createTopicsResult = client.createTopics(newTopicList);
            createTopicsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println(e);
        }
    }

    public static void increasePartitions(AdminClient client, JsonNode config, Set<String> topicList){
        Map<String,NewPartitions> increasePartitionList = new HashMap<>();
        HashMap<String,String> topicHeaderMap = topicHeaderMap(config);

        for (String topic : topicList){
            increasePartitionList.put(topic,NewPartitions.increaseTo(config.get("topics").get(topicHeaderMap.get(topic)).get("partitions").intValue()));
        }

        try {
            final CreatePartitionsResult createPartitionsResult = client.createPartitions(increasePartitionList);
            createPartitionsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println(e);
        }
    }

    public static void deleteTopics(AdminClient client, Collection<String> deleteList) {
        try {
            final DeleteTopicsResult deleteTopicsResult = client.deleteTopics(deleteList);
            deleteTopicsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println(e);
        }
    }

    public static HashMap<String, Set<String>> prepareTopics(AdminClient client, JsonNode config) {
        try {
            //Get Current Topics & Partitions via AdminClient
            Set<String> currentTopics = client.listTopics().names().get();
            HashMap<String,Integer> currentPartitions = new HashMap<>();
            for (String topic : currentTopics) {
                currentPartitions.put(topic, client.describeTopics(currentTopics).values().get(topic).get().partitions().size());
            }

            //Get configured topics & partitions
            Set<String> configuredTopics = new HashSet<>();
            HashMap<String, Integer> configuredPartitions = new HashMap<>();
            for (JsonNode topic : config.get("topics")){
                configuredTopics.add(topic.get("name").textValue());
                configuredPartitions.put(topic.get("name").textValue(), topic.get("partitions").intValue());
            }

            // Determine Partitions configured to be increased
            // Keep the "current" topic/partitions that are in the configured list to be compared
            currentPartitions.keySet().retainAll(configuredPartitions.keySet());
            // Compare configured partitions vs "current" partitions and create modifyPartitions list
            HashSet<String> increasePartitions = new HashSet<>();
            for (Map.Entry current : currentPartitions.entrySet()){
                if (configuredPartitions.get(current.getKey()).intValue() > Integer.valueOf(current.getValue().toString())){
                    increasePartitions.add(current.getKey().toString());
                }
            }

            //Determine topics to remove
            //Commenting out -- all topic deletion will be done manually
            //Set<String> removeTopics = new HashSet<>(currentTopics);
            //removeTopics.removeAll(configuredTopics);

            //Determine topics to add
            Set<String> addTopics = new HashSet<>(configuredTopics);
            addTopics.removeAll(currentTopics);

            HashMap<String,Set<String>> topicPlan = new HashMap<>();
            topicPlan.put("createTopicList", addTopics);
            topicPlan.put("increasePartitionList", increasePartitions);
            //Commenting out deleteTopicList -- all topic deletion will be done manually
            //topicPlan.put("deleteTopicList", removeTopics);

            return topicPlan;

        } catch (InterruptedException | ExecutionException e) {
            System.out.println(e);
            return null;
        }
    }
}
