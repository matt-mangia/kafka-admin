package kafkaadmin.mdsclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafkaadmin.model.AclRule;
import kafkaadmin.model.CentralizedAclBinding;
import kafkaadmin.model.RoleBindingResource;
import okhttp3.*;

import java.io.IOException;
import java.util.*;
// Simple http client for MDS access
public class MDSClient {
    private final String verpath = "/security/1.0";
    private String baseurl;
    private String token;
    private static final MediaType JSON
            = MediaType.get("application/json");
    private OkHttpClient client;
    public String authenticate(String url, String username, String password)
    {
        baseurl = url;
        client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(baseurl+verpath+"/authenticate")
                .addHeader("Authorization", Credentials.basic(username, password))
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) System.err.println("Unexpected code " + response);
            System.err.println(response.code());
            Map<String, List<String>> headers = response.headers().toMultimap();
            token = headers.get("Set-Cookie").get(0).split(";")[0].replace("auth_token=","");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return token;
    }
    public Boolean addRoleForPrincipal(String principal, String role, Map<String, Map<String,String>> scope)
    {
        ObjectMapper mapperObj = new ObjectMapper();
        String json = null;
        Boolean result = false;
        try {
            json = mapperObj.writeValueAsString(scope);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        RequestBody body = RequestBody.create(json, JSON);
        Request request = new Request.Builder()
                .url(baseurl+verpath+"/principals/"+principal+ "/roles/" + role)
                .addHeader("Authorization", "Bearer " + token)
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful())
                System.err.println("Unexpected code " + response);
            else
                result = true;
            System.err.println(response.code());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
    public Boolean addRoleResourcesForPrincipal(String principal, String role, Map<String, Map<String,String>> scope,
                                                ArrayList<RoleBindingResource> resourcesRequest)
    {
        ObjectMapper mapperObj = new ObjectMapper();
        String json = null;
        Boolean result = false;
        Map<String, Object> scopeMap  = new HashMap<>();
        scopeMap.put("resourcePatterns", resourcesRequest);
        scopeMap.put("scope", scope);
        try {
            json = mapperObj.writeValueAsString(scopeMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        RequestBody body = RequestBody.create(json, JSON);
        Request request = new Request.Builder()
                .url(baseurl+verpath+"/principals/"+principal+ "/roles/" + role + "/bindings")
                .addHeader("Authorization", "Bearer " + token)
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful())
                System.err.println("Unexpected code " + response);
            else
                result = true;
            System.err.println(response.code());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
    public Boolean deleteRoleForPrincipal(String principal, String role, Map<String, Map<String,String>> scope)
    {
        ObjectMapper mapperObj = new ObjectMapper();
        String json = null;
        Boolean result = false;
        try {
            json = mapperObj.writeValueAsString(scope);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        RequestBody body = RequestBody.create(json, JSON);
        Request request = new Request.Builder()
                .url(baseurl+verpath+"/principals/"+principal+ "/roles/" + role)
                .addHeader("Authorization", "Bearer " + token)
                .delete(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful())
                System.err.println("Unexpected code " + response);
            else
                result = true;
            System.err.println(response.code());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public Boolean deleteRoleResourcesForPrincipal(String principal, String role, Map<String,Map<String,String>> scope,
                                                   ArrayList<RoleBindingResource> resourcesRequest)
    {
        ObjectMapper mapperObj = new ObjectMapper();
        String json = null;
        Boolean result = false;
        Map<String, Object> scopeMap  = new HashMap<>();
        scopeMap.put("resourcePatterns", resourcesRequest);
        scopeMap.put("scope", scope);
        try {
            json = mapperObj.writeValueAsString(scopeMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        RequestBody body = RequestBody.create(json, JSON);
        Request request = new Request.Builder()
                .url(baseurl+verpath+"/principals/"+principal+ "/roles/" + role + "/bindings")
                .addHeader("Authorization", "Bearer " + token)
                .delete(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful())
                System.err.println("Unexpected code " + response);
            else
                result = true;
            System.err.println(response.code());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public Boolean addCentralizedAcls(Map<String, String> resourcePattern, Map<String, Map<String,String>> scope,
                                      ArrayList<AclRule> aclRules)
    {
        ObjectMapper mapperObj = new ObjectMapper();
        String json = null;
        Boolean result = false;
        AclRule aclRule = aclRules.get(0);
        // API very sensitive to case for ENUM type fields
        aclRule.operation = aclRule.operation.toUpperCase();
        aclRule.permissionType = aclRule.permissionType.toUpperCase();
        Map<String, Object> scopeMap  = new HashMap<>();
        Map<String, Object> filterMap  = new HashMap<>();
        resourcePattern.put("resourceType", resourcePattern.get("resourceType").toUpperCase());
        resourcePattern.put("patternType", resourcePattern.get("patternType").toUpperCase());
        filterMap.put("pattern", resourcePattern);
        filterMap.put("entry", aclRule);
        scopeMap.put("aclBinding", filterMap);
        scopeMap.put("scope", scope);
        try {
            json = mapperObj.writeValueAsString(scopeMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        RequestBody body = RequestBody.create(json, JSON);
        Request request = new Request.Builder()
                .url(baseurl+verpath+"/acls")
                .addHeader("Authorization", "Bearer " + token)
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful())
                System.err.println("Unexpected code " + response);
            else
                result = true;
            System.err.println(response.code());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public Boolean deleteCentralizedAcls(Map<String, String> resourcePattern, Map<String, Map<String,String>> scope,
                                         ArrayList<AclRule> aclRules)
    {
        ObjectMapper mapperObj = new ObjectMapper();
        String json = null;
        Boolean result = false;
        AclRule aclRule = aclRules.get(0);
        Map<String, Object> scopeMap  = new HashMap<>();
        Map<String, Object> filterMap  = new HashMap<>();
        // API very sensitive to case for ENUM type fields
        aclRule.operation = aclRule.operation.toUpperCase();
        aclRule.permissionType = aclRule.permissionType.toUpperCase();
        resourcePattern.put("resourceType", resourcePattern.get("resourceType").toUpperCase());
        resourcePattern.put("patternType", resourcePattern.get("patternType").toUpperCase());
        filterMap.put("patternFilter", resourcePattern);
        filterMap.put("entryFilter", aclRule);
        scopeMap.put("aclBindingFilter", filterMap);
        scopeMap.put("scope", scope);
        try {
            json = mapperObj.writeValueAsString(scopeMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        RequestBody body = RequestBody.create(json, JSON);
        Request request = new Request.Builder()
                .url(baseurl+verpath+"/acls")
                .addHeader("Authorization", "Bearer " + token)
                .delete(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful())
                System.err.println("Unexpected code " + response);
            else
                result = true;
            System.err.println(response.code());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}