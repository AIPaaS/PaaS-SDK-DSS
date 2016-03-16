package com.ai.paas.ipaas.dss.impl;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import redis.clients.jedis.JedisCluster;

import com.ai.paas.ipaas.dss.exception.DSSRuntimeException;
import com.ai.paas.ipaas.dss.interfaces.IDSSClient;
import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class DSSClient implements IDSSClient {

	private static final Logger log = LogManager.getLogger(DSSClient.class);
	private final static String MONGO_DB_NAME = "dbName";
	private final static String MONGO_DB_SIZE = "size";
	private final static String MONGO_FILE_LIMIT_SIZE = "limitSize";
	private static final String FILE_NAME = "filename";

	private final static String REMARK = "remark";
	private MongoClient mongoClient;
	private DB db;
	private static String dbName;
	private static double dbSize;
	private static double fileLimitSize;
	// private static String redisHosts;
	private static JedisCluster jc;
	private static String redisKey;

	public DSSClient(String hosts, String userId, String username,
			String password, String redisHosts,
			Map<String, String> DSSRedisConfMap) {
		MongoCredential credential = MongoCredential.createCredential(username,
				userId, password.toCharArray());
		mongoClient = new MongoClient(DSSHelper.Str2SAList(hosts),
				Arrays.asList(credential));
		// db = mongoClient.getDatabase(userId);
		db = mongoClient.getDB(userId);
		this.dbName = DSSRedisConfMap.get(MONGO_DB_NAME);
		this.fileLimitSize = Double.parseDouble(DSSRedisConfMap
				.get(MONGO_FILE_LIMIT_SIZE));
		this.dbSize = Double.parseDouble(DSSRedisConfMap.get(MONGO_DB_SIZE));
		this.jc = DSSHelper.getRedis(redisHosts);
		this.redisKey = userId + dbName;
	}

	@Override
	public String save(File file, String remark) {
		long usedSize = jc.incrBy(redisKey, 0);
		if (DSSHelper.okSize(DSSHelper.M2byte(fileLimitSize),
				DSSHelper.getFileSize(file)) < 0) {
			log.error("file too large");
			throw new DSSRuntimeException(new Exception("file too large"));
		}
		if (DSSHelper.okSize(
				DSSHelper.okSize(DSSHelper.M2byte(dbSize), usedSize),
				DSSHelper.getFileSize(file)) < 0) {
			log.error("left size not enough");
			throw new DSSRuntimeException(new Exception("left size not enough"));
		}
		String fileType = DSSHelper.getFileType(file.getName());
		GridFS fs = new GridFS(db, dbName);
		GridFSInputFile dbFile;
		try {
			dbFile = fs.createFile(file);
			DBObject dbo = new BasicDBObject();
			dbo.put(REMARK, remark);
			dbFile.setMetaData(dbo);
			dbFile.setContentType(fileType);
			dbFile.save();
			jc.incrBy(redisKey,
					Integer.parseInt(DSSHelper.getFileSize(file) + ""));
			return dbFile.getId().toString();
		} catch (Exception e) {
			log.error(e.toString());
			log.error(e);
			throw new DSSRuntimeException(e);
		}
	}

	@Override
	public String save(byte[] bytes, String remark) {
		if (bytes == null) {
			log.error("bytes illegal");
			throw new DSSRuntimeException(new Exception("bytes illegal"));
		}
		long usedSize = jc.incrBy(redisKey, 0);
		if (DSSHelper.okSize(DSSHelper.M2byte(fileLimitSize),
				DSSHelper.getFileSize(bytes)) < 0) {
			log.error("file too large");
			throw new DSSRuntimeException(new Exception("file too large"));
		}
		if (DSSHelper.okSize(
				DSSHelper.okSize(DSSHelper.M2byte(dbSize), usedSize),
				DSSHelper.getFileSize(bytes)) < 0) {
			log.error("left size not enough");
			throw new DSSRuntimeException(new Exception("left size not enough"));
		}
		GridFS fs = new GridFS(db, dbName);
		GridFSInputFile dbFile;
		try {
			dbFile = fs.createFile(bytes);
			DBObject dbo = new BasicDBObject();
			dbo.put(REMARK, remark);
			dbFile.setMetaData(dbo);
			dbFile.save();
			jc.incrBy(redisKey,
					Integer.parseInt(DSSHelper.getFileSize(bytes) + ""));
			return dbFile.getId().toString();
		} catch (Exception e) {
			log.error(e.toString());
			throw new DSSRuntimeException(e);
		}
	}

	@Override
	public byte[] read(String id) {
		if (id == null || "".equals(id)) {
			log.error("id illegal");
			throw new DSSRuntimeException(new Exception("id illegal"));
		}
		GridFS fs = new GridFS(db, dbName);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(id));
		if (dbFile == null) {
			return null;
		}
		try {
			return DSSHelper.toByteArray(dbFile.getInputStream());
		} catch (Exception e) {
			log.error(e.toString());
			throw new DSSRuntimeException(e);
		}
	}

	@Override
	public boolean delete(String id) {
		// long usedSize = jc.incrBy(redisKey,0);
		if (id == null || "".equals(id)) {
			log.error("id illegal");
			throw new DSSRuntimeException(new Exception("id or bytes illegal"));
		}
		GridFS fs = new GridFS(db, dbName);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(id));
		if (dbFile == null) {
			return false;
		}
		fs.remove(dbFile);
		jc.decrBy(redisKey, Integer.parseInt(dbFile.getLength() + ""));
		return true;
	}

	@Override
	public void update(String id, byte[] bytes) {
		long usedSize = jc.incrBy(redisKey, 0);
		if (bytes == null || id == null || "".equals(id)) {
			log.error("id or bytes illegal");
			throw new DSSRuntimeException(new Exception("id or bytes illegal"));
		}
		if (DSSHelper.okSize(DSSHelper.M2byte(fileLimitSize),
				DSSHelper.getFileSize(bytes)) < 0) {
			log.error("file too large");
			throw new DSSRuntimeException(new Exception("file too large"));
		}
		GridFS fs = new GridFS(db, dbName);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(id));
		if (dbFile == null) {
			log.error("file missing");
			throw new DSSRuntimeException(new Exception("file missing"));
		}
		String fileName = dbFile.getFilename();
		String fileType = dbFile.getContentType();
		usedSize = jc.decrBy(redisKey,
				Integer.parseInt(dbFile.getLength() + ""));
		if (DSSHelper.okSize(
				DSSHelper.okSize(DSSHelper.M2byte(dbSize), usedSize),
				DSSHelper.getFileSize(bytes)) < 0) {
			log.error("left size not enough");
			throw new DSSRuntimeException(new Exception("left size not enough"));
		}
		fs.remove(dbFile);
		GridFSInputFile file = fs.createFile(bytes);
		file.setId(new ObjectId(id));
		file.setContentType(fileType);
		file.setFilename(fileName);
		file.put(FILE_NAME, fileName);
		file.save();
		jc.incrBy(redisKey, Integer.parseInt(DSSHelper.getFileSize(bytes) + ""));
	}

	@Override
	public Date getLastUpdateTime(String id) {
		if (id == null) {
			log.error("id is null");
			throw new DSSRuntimeException(new Exception("id illegal"));
		}
		GridFS fs = new GridFS(db, dbName);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(id));
		if (dbFile == null) {
			log.error("file missing");
			throw new DSSRuntimeException(new Exception("file missing"));
		}
		return dbFile.getUploadDate();
	}

	@Override
	public void update(String id, File file) {
		long usedSize = jc.incrBy(redisKey, 0);
		if (file == null || id == null || "".equals(id)) {
			log.error("id or file illegal");
			throw new DSSRuntimeException(new Exception("id or file illegal"));
		}
		if (DSSHelper.okSize(DSSHelper.M2byte(fileLimitSize),
				DSSHelper.getFileSize(file)) < 0) {
			log.error("file too large");
			throw new DSSRuntimeException(new Exception("file too large"));
		}
		GridFS fs = new GridFS(db, dbName);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(id));
		if (dbFile == null) {
			log.error("file missing");
			throw new DSSRuntimeException(new Exception("file missing"));
		}
		String fileName = dbFile.getFilename();
		String fileType = dbFile.getContentType();
		if (DSSHelper.okSize(
				DSSHelper.okSize(DSSHelper.M2byte(dbSize),
						usedSize - Integer.parseInt(dbFile.getLength() + "")),
				DSSHelper.getFileSize(file)) < 0) {
			log.error("left size not enough");
			throw new DSSRuntimeException(new Exception("left size not enough"));
		}
		usedSize = jc.decrBy(redisKey,
				Integer.parseInt(dbFile.getLength() + ""));
		fs.remove(dbFile);
		GridFSInputFile fsfile = null;
		try {
			fsfile = fs.createFile(file);
		} catch (IOException e) {
			log.error(e.getMessage());
			throw new DSSRuntimeException(new Exception(e));
		}
		fsfile.setId(new ObjectId(id));
		fsfile.setContentType(fileType);
		fsfile.setFilename(fileName);
		fsfile.put(FILE_NAME, fileName);
		fsfile.save();
		jc.incrBy(redisKey, Integer.parseInt(DSSHelper.getFileSize(file) + ""));
	}

	public static void main(String[] args) {
		byte[] b = "哈喽我的asd123".getBytes();
		Gson gson = new Gson();
		Map m = new HashMap();
		m.put("bytes", b);
		System.out.println(gson.toJson(m));
		Map mp = gson.fromJson(gson.toJson(m), Map.class);
		List<Double> l = (List<Double>) mp.get("bytes");
		byte[] bytes = new byte[l.size()];
		for (int i = 0; i < l.size(); i++) {
			bytes[i] = new BigDecimal((double) l.get(i)).byteValue();
		}
		System.out.println(new String(bytes));
	}
}
