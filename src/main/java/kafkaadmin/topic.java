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

    public static void createTopics(AdminClient client, JsonNode config, Set<String> topicList) {
        Collection<NewTopic> newTopicList = new ArrayList<>();

        JsonNode topic = config.get("topics");
        for (String t : topicList) {
            JsonNode topicConfigs = topic.get(t);
            NewTopic newTopic = new NewTopic(topicConfigs.get("name").asText(), topicConfigs.get("partitions").asInt(), topicConfigs.get("replication.factor").shortValue());

            Map<String,String> customConfigs = new HashMap<>();
            Iterator<Map.Entry<String,JsonNode>> iterator = topicConfigs.fields();
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
            //Get Current Topics via AdminClient
            Set<String> currentTopics = client.listTopics().names().get();

            //Get configured topics + defaults
            Set<String> configuredTopics = new HashSet<>();
            for (JsonNode topic : config.get("topics")){
                configuredTopics.add(topic.get("name").textValue());
            }
            for (JsonNode topic : config.get("default_topics")){
                configuredTopics.add(topic.textValue());
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
            //Commenting out deleteTopicList -- all topic deletion will be done manually
            //topicPlan.put("deleteTopicList", removeTopics);

            return topicPlan;

        } catch (InterruptedException | ExecutionException e) {
            System.out.println(e);
            return null;
        }
    }
}
