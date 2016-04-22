package com.ai.paas.ipaas.dss.impl;

import java.io.File;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import redis.clients.jedis.JedisCluster;

import com.ai.paas.ipaas.dss.base.DSSBaseFactory;
import com.ai.paas.ipaas.dss.base.MongoInfo;
import com.ai.paas.ipaas.dss.base.exception.DSSRuntimeException;
import com.ai.paas.ipaas.dss.base.impl.DSSClient;
import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;
import com.google.gson.Gson;

public class DSSSrvClient implements IDSSClient {

	private static final Logger log = LogManager.getLogger(DSSSrvClient.class);
	private final static String MONGO_DB_NAME = "dbName";
	private final static String MONGO_DB_SIZE = "size";
	private final static String MONGO_FILE_LIMIT_SIZE = "limitSize";

	private String bucket;
	private double dbSize;
	private double fileLimitSize;
	private JedisCluster jc;
	private String redisKey;
	private DSSClient dssClient = null;

	public DSSSrvClient(String hosts, String userId, String username,
			String password, String redisHosts,
			Map<String, String> DSSRedisConfMap) throws Exception {
		this.bucket = DSSRedisConfMap.get(MONGO_DB_NAME);
		this.fileLimitSize = Double.parseDouble(DSSRedisConfMap
				.get(MONGO_FILE_LIMIT_SIZE));
		this.dbSize = Double.parseDouble(DSSRedisConfMap.get(MONGO_DB_SIZE));

		MongoInfo mongoInfo = new MongoInfo(hosts, userId, username, password,
				bucket);
		Gson gson = new Gson();
		// 需要变成json格式
		dssClient = (DSSClient) DSSBaseFactory
				.getClient(gson.toJson(mongoInfo));

		this.jc = DSSSrvHelper.getRedis(redisHosts);
		this.redisKey = userId + bucket;
	}

	@Override
	public String save(File file, String remark) {
		long usedSize = jc.incrBy(redisKey, 0);
		if (DSSSrvHelper.okSize(DSSSrvHelper.M2byte(fileLimitSize),
				DSSSrvHelper.getFileSize(file)) < 0) {
			log.error("file too large");
			throw new DSSRuntimeException(new Exception("file too large"));
		}
		if (DSSSrvHelper.okSize(
				DSSSrvHelper.okSize(DSSSrvHelper.M2byte(dbSize), usedSize),
				DSSSrvHelper.getFileSize(file)) < 0) {
			log.error("left size not enough");
			throw new DSSRuntimeException(new Exception("left size not enough"));
		}
		try {
			String fileId = dssClient.save(file, remark);
			jc.incrBy(redisKey,
					Integer.parseInt(DSSSrvHelper.getFileSize(file) + ""));
			return fileId;
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
		if (DSSSrvHelper.okSize(DSSSrvHelper.M2byte(fileLimitSize),
				DSSSrvHelper.getFileSize(bytes)) < 0) {
			log.error("file too large");
			throw new DSSRuntimeException(new Exception("file too large"));
		}
		if (DSSSrvHelper.okSize(
				DSSSrvHelper.okSize(DSSSrvHelper.M2byte(dbSize), usedSize),
				DSSSrvHelper.getFileSize(bytes)) < 0) {
			log.error("left size not enough");
			throw new DSSRuntimeException(new Exception("left size not enough"));
		}
		try {
			String fileId = dssClient.save(bytes, remark);
			jc.incrBy(redisKey,
					Integer.parseInt(DSSSrvHelper.getFileSize(bytes) + ""));
			return fileId;
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
		try {
			return dssClient.read(id);
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
		long fileSize = dssClient.getFileSize(id);
		dssClient.delete(id);
		jc.decrBy(redisKey, Integer.parseInt(fileSize + ""));
		return true;
	}

	@Override
	public void update(String id, byte[] bytes) {
		long usedSize = jc.incrBy(redisKey, 0);
		if (bytes == null || id == null || "".equals(id)) {
			log.error("id or bytes illegal");
			throw new DSSRuntimeException(new Exception("id or bytes illegal"));
		}
		if (DSSSrvHelper.okSize(DSSSrvHelper.M2byte(fileLimitSize),
				DSSSrvHelper.getFileSize(bytes)) < 0) {
			log.error("file too large");
			throw new DSSRuntimeException(new Exception("file too large"));
		}
		usedSize = jc.decrBy(redisKey,
				Integer.parseInt(dssClient.getFileSize(id) + ""));
		if (DSSSrvHelper.okSize(
				DSSSrvHelper.okSize(DSSSrvHelper.M2byte(dbSize), usedSize),
				DSSSrvHelper.getFileSize(bytes)) < 0) {
			log.error("left size not enough");
			throw new DSSRuntimeException(new Exception("left size not enough"));
		}
		dssClient.delete(id);
		dssClient.save(bytes, null);
		jc.incrBy(redisKey,
				Integer.parseInt(DSSSrvHelper.getFileSize(bytes) + ""));
	}

	@Override
	public Date getLastUpdateTime(String id) {
		return dssClient.getLastUpdateTime(id);
	}

	@Override
	public void update(String id, File file) {
		long usedSize = jc.incrBy(redisKey, 0);
		if (file == null || id == null || "".equals(id)) {
			log.error("id or file illegal");
			throw new DSSRuntimeException(new Exception("id or file illegal"));
		}
		if (DSSSrvHelper.okSize(DSSSrvHelper.M2byte(fileLimitSize),
				DSSSrvHelper.getFileSize(file)) < 0) {
			log.error("file too large");
			throw new DSSRuntimeException(new Exception("file too large"));
		}
		long fileSize = dssClient.getFileSize(id);
		if (DSSSrvHelper.okSize(
				DSSSrvHelper.okSize(DSSSrvHelper.M2byte(dbSize), usedSize
						- Integer.parseInt(fileSize + "")),
				DSSSrvHelper.getFileSize(file)) < 0) {
			log.error("left size not enough");
			throw new DSSRuntimeException(new Exception("left size not enough"));
		}
		usedSize = jc.decrBy(redisKey, Integer.parseInt(fileSize + ""));
		dssClient.delete(id);
		dssClient.save(file, null);
		jc.incrBy(redisKey,
				Integer.parseInt(DSSSrvHelper.getFileSize(file) + ""));
	}

	@Override
	public long getFileSize(String id) {
		return dssClient.getFileSize(id);
	}

	@Override
	public boolean isFileExist(String id) {
		return dssClient.isFileExist(id);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
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
