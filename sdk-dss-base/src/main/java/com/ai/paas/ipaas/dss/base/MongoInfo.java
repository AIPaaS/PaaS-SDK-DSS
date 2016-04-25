package com.ai.paas.ipaas.dss.base;

import java.io.Serializable;

public class MongoInfo implements Serializable {
	private static final long serialVersionUID = 538662539677622330L;
	private String mongoServer = null;
	private String database = null;
	private String userName = null;
	private String password = null;
	private String bucket = null;

	public MongoInfo(String servers, String dataBase, String user,
			String password, String bucket) {
		this.mongoServer = servers;
		this.database = dataBase;
		this.userName = user;
		this.password = password;
		this.bucket = bucket;
	}

	public String getMongoServer() {
		return mongoServer;
	}

	public void setMongoServer(String mongoServer) {
		this.mongoServer = mongoServer;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getBucket() {
		return bucket;
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

}
