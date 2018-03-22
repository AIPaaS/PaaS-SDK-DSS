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
import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;
import com.ai.paas.ipaas.util.Assert;
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
	private IDSSClient dssClient = null;

	public DSSSrvClient(String hosts, String userId, String username, String password, String redisHosts,
			Map<String, String> DSSRedisConfMap) throws Exception {
		this.bucket = DSSRedisConfMap.get(MONGO_DB_NAME);
		this.fileLimitSize = Double.parseDouble(DSSRedisConfMap.get(MONGO_FILE_LIMIT_SIZE));
		this.dbSize = Double.parseDouble(DSSRedisConfMap.get(MONGO_DB_SIZE));

		MongoInfo mongoInfo = new MongoInfo(hosts, userId, username, password, bucket);
		Gson gson = new Gson();
		// 需要变成json格式
		dssClient = (IDSSClient) DSSBaseFactory.getClient(gson.toJson(mongoInfo));

		this.jc = DSSSrvHelper.getRedis(redisHosts);
		this.redisKey = userId + bucket;
		// 此处需要同步一下mongodb已使用的数据到redis中去，以免redis重启丢失，同时进行校正
		long size = dssClient.getSize();
		if (size > 0) {
			System.out.println(jc.get(redisKey));
			jc.set(redisKey, size + "");
			System.out.println(jc.get(redisKey));
		}
	}

	@Override
	public String save(File file, String remark) {
		judgeSize(file);
		try {
			String fileId = dssClient.save(file, remark);
			jc.incrBy(redisKey, Integer.parseInt(DSSSrvHelper.getFileSize(file) + ""));
			return fileId;
		} catch (Exception e) {
			log.error("", e);
			throw new DSSRuntimeException(e);
		}
	}

	@Override
	public String save(byte[] bytes, String remark) {
		if (bytes == null) {
			log.error("bytes illegal");
			throw new DSSRuntimeException(new Exception("bytes illegal"));
		}
		judgeSize(bytes);
		try {
			String fileId = dssClient.save(bytes, remark);
			jc.incrBy(redisKey, Integer.parseInt(DSSSrvHelper.getFileSize(bytes) + ""));
			return fileId;
		} catch (Exception e) {
			log.error("", e);
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
			log.error("", e);
			throw new DSSRuntimeException(e);
		}
	}

	@Override
	public boolean delete(String id) {
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
	public String update(String id, byte[] bytes) {
		if (bytes == null || id == null || "".equals(id)) {
			log.error("id or bytes illegal");
			throw new DSSRuntimeException(new Exception("id or bytes illegal"));
		}
		judgeSize(bytes);
		dssClient.delete(id);
		jc.incrBy(redisKey, Integer.parseInt(DSSSrvHelper.getFileSize(bytes) + ""));
		return dssClient.save(bytes, null);
	}

	@Override
	public Date getLastUpdateTime(String id) {
		return dssClient.getLastUpdateTime(id);
	}

	@Override
	public String update(String id, File file) {

		if (file == null || id == null || "".equals(id)) {
			log.error("id or file illegal");
			throw new DSSRuntimeException(new Exception("id or file illegal"));
		}
		long fileSize = dssClient.getFileSize(id);
		judgeSize(file);
		jc.decrBy(redisKey, Integer.parseInt(fileSize + ""));
		dssClient.delete(id);
		String newid = dssClient.save(file, null);
		jc.incrBy(redisKey, Integer.parseInt(DSSSrvHelper.getFileSize(file) + ""));
		return newid;
	}

	@Override
	public long getFileSize(String id) {
		return dssClient.getFileSize(id);
	}

	@Override
	public boolean isFileExist(String id) {
		return dssClient.isFileExist(id);
	}

	private void judgeSize(Object obj) {
		long usedSize = jc.incrBy(redisKey, 0);
		if (DSSSrvHelper.okSize(DSSSrvHelper.M2byte(fileLimitSize), DSSSrvHelper.getSize(obj)) < 0) {
			log.error("file too large");
			throw new DSSRuntimeException(new Exception("file too large"));
		}

		if (DSSSrvHelper.okSize(DSSSrvHelper.okSize(DSSSrvHelper.M2byte(dbSize), usedSize),
				DSSSrvHelper.getSize(obj)) < 0) {
			log.error("left size not enough");
			throw new DSSRuntimeException(new Exception("left size not enough"));
		}
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

	@Override
	public String insert(String content) {
		Assert.notNull(content, "content is null");
		judgeSize(content);
		String id = dssClient.insert(content);
		jc.incrBy(redisKey, Integer.parseInt(DSSSrvHelper.getSize(content) + ""));
		return id;
	}

	@Override
	public String insertJSON(String doc) {
		Assert.notNull(doc, "content is null");
		judgeSize(doc);
		String id = dssClient.insertJSON(doc);
		jc.incrBy(redisKey, Integer.parseInt(DSSSrvHelper.getSize(doc) + ""));
		return id;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public String insert(Map doc) {
		Assert.notNull(doc, "content is null");
		judgeSize(doc);
		String id = dssClient.insert(doc);
		jc.incrBy(redisKey, Integer.parseInt(DSSSrvHelper.getSize(doc) + ""));
		return id;
	}

	@Override
	public void insertBatch(List<Map<String, Object>> docs) {
		Assert.notNull(docs, "content is null");
		judgeSize(docs);
		dssClient.insertBatch(docs);
		jc.incrBy(redisKey, Integer.parseInt(DSSSrvHelper.getSize(docs) + ""));
	}

	@Override
	public long deleteById(String id) {
		Assert.notNull(id, "id is null");
		String content = dssClient.findById(id);
		long rows = dssClient.deleteById(id);
		jc.decrBy(redisKey, Integer.parseInt(DSSSrvHelper.getSize(content) + ""));
		return rows;
	}

	@Override
	public long deleteByJson(String doc) {
		Assert.notNull(doc, "doc is null");
		long rows = dssClient.deleteByJson(doc);
		return rows;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public long deleteByMap(Map doc) {
		Assert.notNull(doc, "doc is null");
		long rows = dssClient.deleteByMap(doc);
		return rows;
	}

	@Override
	public long deleteAll() {
		long rows = dssClient.deleteAll();
		return rows;
	}

	@Override
	public long deleteBatch(List<Map<String, Object>> docs) {
		Assert.notNull(docs, "docs is null");
		long rows = dssClient.deleteBatch(docs);
		return rows;
	}

	@Override
	public long updateById(String id, String doc) {
		Assert.notNull(id, "id is null");
		Assert.notNull(doc, "doc is null");
		long rows = dssClient.updateById(id, doc);
		return rows;
	}

	@Override
	public long update(String query, String doc) {

		Assert.notNull(query, "query is null");
		Assert.notNull(doc, "doc is null");
		long rows = dssClient.update(query, doc);
		return rows;
	}

	@Override
	public long upsert(String query, String doc) {
		Assert.notNull(query, "query is null");
		Assert.notNull(doc, "doc is null");
		long rows = dssClient.upsert(query, doc);
		return rows;
	}

	@Override
	public String findById(String id) {
		Assert.notNull(id, "id is null");
		return dssClient.findById(id);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public String find(Map doc) {
		Assert.notNull(doc, "doc is null");
		if (doc.size() <= 0)
			throw new IllegalArgumentException();
		return dssClient.find(doc);
	}

	@Override
	public String find(String query) {
		Assert.notNull(query, "query is null");
		return dssClient.find(query);
	}

	@Override
	public String query(String query, int pageNumber, int pageSize) {
		Assert.notNull(query, "query is null");
		return dssClient.query(query, pageNumber, pageSize);
	}

	@SuppressWarnings("deprecation")
	@Override
	public long getCount(String query) {
		Assert.notNull(query, "query is null");
		return dssClient.getCount(query);
	}

	@Override
	public void addIndex(String field, boolean unique) {
		Assert.notNull(field, "field is null");
		dssClient.addIndex(field, unique);
	}

	@Override
	public void dropIndex(String field) {
		Assert.notNull(field, "field is null");
		dssClient.dropIndex(field);
	}

	@Override
	public boolean isIndexExist(String field) {
		Assert.notNull(field, "field is null");
		return dssClient.isIndexExist(field);
	}

	@Override
	public Long getSize() {
		return dssClient.getSize();
	}

	@Override
	public boolean collectionExists(String collectionName) {
		return dssClient.collectionExists(collectionName);
	}

	@Override
	public long count(String query) {
		return dssClient.count(query);
	}

	@Override
	public void dropAllIndex() {
		dssClient.dropAllIndex();

	}

	@Override
	public void close() {
		dssClient.close();
	}

	@Override
	public byte[] readByName(String fileName) {
		if (fileName == null || "".equals(fileName)) {
			log.error("fileName illegal");
			throw new DSSRuntimeException(new Exception("fileName illegal"));
		}
		try {
			return dssClient.readByName(fileName);
		} catch (Exception e) {
			log.error("", e);
			throw new DSSRuntimeException(e);
		}
	}

	@Override
	public boolean withinPolygon(String field, String mongoId, double[] point) {
		return dssClient.withinPolygon(field, mongoId, point);
	}

	@Override
	public boolean withinPolygon(String field, List<double[]> coordinates, double[] point) {
		return dssClient.withinPolygon(field, coordinates, point);
	}

	@Override
	public String findGeoWithinPolygon(String field, List<double[]> coordinates) {
		return dssClient.findGeoWithinPolygon(field, coordinates);
	}

	@Override
	public void createGeoIndex(String field) {
		dssClient.createGeoIndex(field);
	}
}
