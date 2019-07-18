package com.ai.paas.ipaas.dss;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ai.paas.ipaas.dss.impl.DSSClient;
import com.ai.paas.util.Assert;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class DSSBaseFactory {

    private static Map<String, IDSSClient> clients = new ConcurrentHashMap<>();
    private static final Logger log = LogManager.getLogger(DSSBaseFactory.class);

    private DSSBaseFactory() {
        // 禁止私有化
    }

    /**
     * @param mongoInfo {"mongoServer":"10.1.xxx.xxx:37017;10.1.xxx.xxx:37017",
     *                  "database":"image","userName":"sa","password":"sa"}
     * @return
     * @throws Exception
     */
    public static IDSSClient getClient(String mongoInfo) throws Exception {
        IDSSClient client = null;
        log.info("Check Formal Parameter AuthDescriptor ...");
        Assert.notNull(mongoInfo, "mongoInfo is null");
        mongoInfo = mongoInfo.trim();
        if (clients.containsKey(mongoInfo)) {
            client = clients.get(mongoInfo);
            return client;
        }
        JsonParser parser = new JsonParser();
        JsonElement je = parser.parse(mongoInfo);
        JsonObject in = je.getAsJsonObject();
        String mongoServer = in.get("mongoServer").getAsString();
        String database = in.get("database").getAsString();
        String userName = in.get("userName").getAsString();
        String password = in.get("password").getAsString();
        String bucket = null;
        if (null != in.get("bucket")) {
            bucket = in.get("bucket").getAsString();
        }
        Assert.notNull(mongoServer, "mongoServer is null");
        Assert.notNull(database, "database is null");
        Assert.notNull(userName, "userName is null");
        Assert.notNull(password, "password is null");
        Assert.notNull(bucket, "bucket(table) is null, pls. set!");
        mongoServer = mongoServer.trim();
        database = database.trim();
        userName = userName.trim();
        password = password.trim();
        bucket = bucket.trim();
        client = new DSSClient(mongoServer, database, userName, password, bucket);
        clients.put(mongoInfo, client);
        return client;
    }

}
