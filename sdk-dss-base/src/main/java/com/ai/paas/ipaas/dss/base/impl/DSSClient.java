package com.ai.paas.ipaas.dss.base.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import com.ai.paas.ipaas.dss.base.exception.DSSRuntimeException;
import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;
import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class DSSClient implements IDSSClient {

	private static final Logger log = LogManager.getLogger(DSSClient.class);
	private static final String FILE_NAME = "filename";

	private final static String REMARK = "remark";
	private String bucket = "fs";
	private MongoClient mongoClient;
	private DB db;
	private String defaultCollection = null;
	private Gson gson = null;

	public DSSClient(String addr, String database, String userName,
			String password, String bucket) {
		MongoCredential credential = MongoCredential.createCredential(userName,
				database, password.toCharArray());
		mongoClient = new MongoClient(DSSHelper.Str2SAList(addr),
				Arrays.asList(credential));
		db = mongoClient.getDB(database);
		// 默认表就是服务标识
		defaultCollection = bucket;
		gson = new Gson();
		if (null != bucket && !"".equals(bucket.trim())) {
			this.bucket = bucket;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#save(java.io.File,
	 * java.lang.String)
	 */
	@Override
	public String save(File file, String remark) {
		String fileType = DSSHelper.getFileType(file.getName());
		GridFS fs = new GridFS(db, bucket);
		GridFSInputFile dbFile;
		try {
			dbFile = fs.createFile(file);
			DBObject dbo = new BasicDBObject();
			dbo.put(REMARK, remark);
			dbFile.setMetaData(dbo);
			dbFile.setContentType(fileType);
			dbFile.save();
			return dbFile.getId().toString();
		} catch (Exception e) {
			log.error(e.toString());
			log.error(e);
			throw new DSSRuntimeException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#save(byte[],
	 * java.lang.String)
	 */
	@Override
	public String save(byte[] bytes, String remark) {
		if (bytes == null) {
			log.error("bytes illegal");
			throw new DSSRuntimeException(new Exception("bytes illegal"));
		}
		GridFS fs = new GridFS(db, bucket);
		GridFSInputFile dbFile;
		try {
			dbFile = fs.createFile(bytes);
			DBObject dbo = new BasicDBObject();
			dbo.put(REMARK, remark);
			dbFile.setMetaData(dbo);
			dbFile.save();
			return dbFile.getId().toString();
		} catch (Exception e) {
			log.error(e.toString());
			throw new DSSRuntimeException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#read(java.lang.String)
	 */
	@Override
	public byte[] read(String id) {
		if (id == null || "".equals(id)) {
			log.error("id illegal");
			throw new DSSRuntimeException(new Exception("id illegal"));
		}
		GridFS fs = new GridFS(db, bucket);
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#delete(java.lang.String)
	 */
	@Override
	public boolean delete(String id) {
		if (id == null || "".equals(id)) {
			log.error("id illegal");
			throw new DSSRuntimeException(new Exception("id or bytes illegal"));
		}
		GridFS fs = new GridFS(db, bucket);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(id));
		if (dbFile == null) {
			return false;
		}
		fs.remove(dbFile);
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#update(java.lang.String,
	 * byte[])
	 */
	@Override
	public void update(String id, byte[] bytes) {
		if (bytes == null || id == null || "".equals(id)) {
			log.error("id or bytes illegal");
			throw new DSSRuntimeException(new Exception("id or bytes illegal"));
		}
		GridFS fs = new GridFS(db, bucket);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(id));
		if (dbFile == null) {
			log.error("file missing");
			throw new DSSRuntimeException(new Exception("file missing"));
		}
		String fileName = dbFile.getFilename();
		String fileType = dbFile.getContentType();
		fs.remove(dbFile);
		GridFSInputFile file = fs.createFile(bytes);
		file.setId(new ObjectId(id));
		file.setContentType(fileType);
		file.setFilename(fileName);
		file.put(FILE_NAME, fileName);
		file.save();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#getLastUpdateTime(java.lang
	 * .String)
	 */
	@Override
	public Date getLastUpdateTime(String id) {
		if (id == null) {
			log.error("id is null");
			throw new DSSRuntimeException(new Exception("id illegal"));
		}
		GridFS fs = new GridFS(db, bucket);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(id));
		if (dbFile == null) {
			log.error("file missing");
			throw new DSSRuntimeException(new Exception("file missing"));
		}
		return dbFile.getUploadDate();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#update(java.lang.String,
	 * java.io.File)
	 */
	@Override
	public void update(String id, File file) {
		if (file == null || id == null || "".equals(id)) {
			log.error("id or file illegal");
			throw new DSSRuntimeException(new Exception("id or file illegal"));
		}
		GridFS fs = new GridFS(db, bucket);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(id));
		if (dbFile == null) {
			log.error("file missing");
			throw new DSSRuntimeException(new Exception("file missing"));
		}
		String fileName = dbFile.getFilename();
		String fileType = dbFile.getContentType();
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
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#getFileSize(java.lang.String)
	 */
	@Override
	public long getFileSize(String id) {
		GridFS fs = new GridFS(db, bucket);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(id));
		if (dbFile != null) {
			return dbFile.getLength();
		}
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#isFileExist(java.lang.String)
	 */
	@Override
	public boolean isFileExist(String id) {
		GridFS fs = new GridFS(db, bucket);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(id));
		if (dbFile != null) {
			return true;
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#insert(java.lang.String)
	 */
	@Override
	public String insert(String content) {
		BasicDBObject doc = new BasicDBObject();
		ObjectId id = new ObjectId();
		doc.put("_id", id);
		doc.put("content", content);
		db.getCollection(defaultCollection).insert(doc);
		return id.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#insertJSON(java.lang.String)
	 */
	@Override
	public String insertJSON(String doc) {
		BasicDBObject dbObj = gson.fromJson(doc, BasicDBObject.class);
		ObjectId id = new ObjectId();
		dbObj.put("_id", id);
		db.getCollection(defaultCollection).insert(dbObj);
		return id.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#insert(java.util.Map)
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public String insert(Map doc) {
		if (null == doc || doc.size() <= 0)
			throw new IllegalArgumentException();
		DBObject dbObj = new BasicDBObject(doc);
		ObjectId id = new ObjectId();
		dbObj.put("_id", id);
		db.getCollection(defaultCollection).insert(dbObj);
		return id.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#insertBatch(java.util.List)
	 */
	@Override
	public void insertBatch(List<Map<String, Object>> docs) {
		if (docs == null || docs.isEmpty()) {
			throw new IllegalArgumentException();
		}
		List<DBObject> documents = new ArrayList<DBObject>();
		for (int i = 0; i < docs.size(); i++) {
			DBObject dbObj = new BasicDBObject(docs.get(i));
			documents.add(dbObj);
		}
		db.getCollection(defaultCollection).insert(documents);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#deleteById(java.lang.String)
	 */
	@Override
	public int deleteById(String id) {
		BasicDBObject query = new BasicDBObject("_id", new ObjectId(id));
		return db.getCollection(defaultCollection).remove(query).getN();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#deleteByJson(java.lang.String)
	 */
	@Override
	public int deleteByJson(String doc) {
		BasicDBObject dbObj = gson.fromJson(doc, BasicDBObject.class);
		return db.getCollection(defaultCollection).remove(dbObj).getN();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#deleteByMap(java.util.Map)
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public int deleteByMap(Map doc) {
		if (null == doc || doc.size() <= 0)
			throw new IllegalArgumentException();
		DBObject dbObj = new BasicDBObject(doc);
		return db.getCollection(defaultCollection).remove(dbObj).getN();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#deleteAll()
	 */
	@Override
	public int deleteAll() {
		// 慎重使用，不可恢复
		DBObject dbObj = new BasicDBObject();
		return db.getCollection(defaultCollection).remove(dbObj).getN();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#deleteBatch(java.util.List)
	 */
	@Override
	public int deleteBatch(List<Map<String, Object>> docs) {
		if (docs == null || docs.isEmpty()) {
			throw new IllegalArgumentException();
		}
		int total = 0;
		for (int i = 0; i < docs.size(); i++) {
			DBObject dbObj = new BasicDBObject(docs.get(i));
			total += db.getCollection(defaultCollection).remove(dbObj).getN();
		}
		return total;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#updateById(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public int updateById(String id, String doc) {
		BasicDBObject query = new BasicDBObject("_id", new ObjectId(id));
		BasicDBObject dbObj = gson.fromJson(doc, BasicDBObject.class);
		DBObject modifiedObject = new BasicDBObject();
		modifiedObject.put("$set", dbObj);
		return db.getCollection(defaultCollection)
				.update(query, modifiedObject).getN();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#update(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public int update(String query, String doc) {
		BasicDBObject qryObj = gson.fromJson(query, BasicDBObject.class);
		BasicDBObject dbObj = gson.fromJson(doc, BasicDBObject.class);
		DBObject modifiedObject = new BasicDBObject();
		modifiedObject.put("$set", dbObj);
		return db.getCollection(defaultCollection)
				.update(qryObj, modifiedObject, false, true).getN();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#updateOrInsert(java.lang.String
	 * , java.lang.String)
	 */
	@Override
	public int updateOrInsert(String query, String doc) {
		BasicDBObject qryObj = gson.fromJson(query, BasicDBObject.class);
		BasicDBObject dbObj = gson.fromJson(doc, BasicDBObject.class);
		DBObject modifiedObject = new BasicDBObject();
		modifiedObject.put("$set", dbObj);
		return db.getCollection(defaultCollection)
				.update(qryObj, modifiedObject, true, true).getN();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#findById(java.lang.String)
	 */
	@Override
	public String findById(String id) {
		DBObject obj = db.getCollection(defaultCollection).findOne(
				new ObjectId(id));
		if (null != obj)
			return gson.toJson(obj);
		else
			return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#findOne(java.util.Map)
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public String findOne(Map doc) {
		DBObject query = new BasicDBObject(doc);
		DBObject obj = db.getCollection(defaultCollection).findOne(query);
		if (null != obj)
			return gson.toJson(obj);
		else
			return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#find(java.lang.String)
	 */
	@Override
	public String find(String query) {
		// 慎用，可能很大量
		BasicDBObject qryObj = gson.fromJson(query, BasicDBObject.class);
		DBCursor cursor = db.getCollection(defaultCollection).find(qryObj);
		if (null != cursor) {
			List<DBObject> result = new ArrayList<>();
			try {
				while (cursor.hasNext()) {
					DBObject obj = cursor.next();
					result.add(obj);
				}
			} finally {
				cursor.close();
			}
			return gson.toJson(result);
		} else
			return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#query(java.lang.String,
	 * int, int)
	 */
	@Override
	public String query(String query, int pageNumber, int pageSize) {
		BasicDBObject qryObj = gson.fromJson(query, BasicDBObject.class);
		DBCursor cursor = db.getCollection(defaultCollection).find(qryObj)
				.skip((pageNumber >= 1 ? (pageNumber - 1) * pageSize : 0))
				.limit(pageSize);
		if (null != cursor) {
			List<DBObject> result = new ArrayList<>();
			try {
				while (cursor.hasNext()) {
					DBObject obj = cursor.next();
					result.add(obj);
				}
			} finally {
				cursor.close();
			}
			return gson.toJson(result);
		} else
			return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#getCount(java.lang.String)
	 */
	@Override
	public long getCount(String query) {
		BasicDBObject qryObj = gson.fromJson(query, BasicDBObject.class);
		return db.getCollection(defaultCollection).count(qryObj);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#addIndex(java.lang.String,
	 * boolean)
	 */
	@Override
	public void addIndex(String field, boolean unique) {
		String indexName = "idx_" + field;
		DBObject dbo = new BasicDBObject(indexName, 1);
		db.getCollection(defaultCollection).createIndex(dbo, indexName, unique);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#dropIndex(java.lang.String)
	 */
	@Override
	public void dropIndex(String field) {
		String indexName = "idx_" + field;
		db.getCollection(defaultCollection).dropIndex(indexName);
	}

	public boolean isIndexExist(String field) {
		String indexName = "idx_" + field;
		List<DBObject> indexs = db.getCollection(defaultCollection)
				.getIndexInfo();
		if (null == indexs || indexs.size() <= 0)
			return false;
		else {
			boolean found = false;
			for (DBObject dbo : indexs) {
				if (indexName.equals(dbo.get("name"))) {
					found = true;
					break;
				}
			}
			return found;
		}
	}

	public Long getSize() {
		long size = -1;
		// 此处需要取得多个的大小
		CommandResult tableResult = db.getCollection(defaultCollection)
				.getStats();
		CommandResult fileResult = db.getCollection(
				defaultCollection + ".chunks").getStats();
		double dataSize = 0;
		double fileSize = 0;
		if (null != tableResult) {
			if (null != tableResult.get("size"))
				dataSize = tableResult.getDouble("size");
		}
		if (null != fileResult) {
			if (null != fileResult.get("size"))
				fileSize = fileResult.getDouble("size");
		}
		size = Math.round(dataSize + fileSize);
		return size;
	}

	public static void main(String[] args) {
		IDSSClient dss = new DSSClient("10.1.228.200:37017;10.1.228.202:37017",
				"admin", "sa", "sa", "fs");
		byte[] byte0 = "123456789".getBytes();
		String str1 = "thenormaltest";
		dss.save(byte0, str1);
	}
}
