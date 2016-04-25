package com.ai.paas.ipaas.dss.base;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ai.paas.ipaas.dss.base.impl.DSSClient;
import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;
import com.ai.paas.ipaas.util.Assert;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class DSSBaseFactory {

	private static Map<String, IDSSClient> DSSClients = new ConcurrentHashMap<>();
	private static final Logger log = LogManager
			.getLogger(DSSBaseFactory.class);

	private DSSBaseFactory() {
		// 禁止私有化
	}

	/**
	 * @param mongoInfo
	 *            {"mongoServer":"10.1.xxx.xxx:37017;10.1.xxx.xxx:37017",
	 *            "database":"image","userName":"sa","password":"sa"}
	 * @return
	 * @throws Exception
	 */
	public static IDSSClient getClient(String mongoInfo) throws Exception {
		IDSSClient DSSClient = null;
		log.info("Check Formal Parameter AuthDescriptor ...");
		Assert.notNull(mongoInfo, "mongoInfo is null");
		mongoInfo = mongoInfo.trim();
		if (DSSClients.containsKey(mongoInfo)) {
			DSSClient = DSSClients.get(mongoInfo);
			return DSSClient;
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
		mongoServer = mongoServer.trim();
		database = database.trim();
		userName = userName.trim();
		password.trim();
		bucket = bucket.trim();
		DSSClient = new DSSClient(mongoServer, database, userName, password,
				bucket);
		DSSClients.put(mongoInfo, DSSClient);
		return DSSClient;
	}

}
