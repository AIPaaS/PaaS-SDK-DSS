package com.ai.paas.ipaas.dss.base.impl;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import com.ai.paas.ipaas.dss.base.exception.DSSRuntimeException;
import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;
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
	private static final String FILE_NAME = "filename";

	private final static String REMARK = "remark";
	private String bucket = "fs";
	private MongoClient mongoClient;
	private DB db;

	public DSSClient(String addr, String database, String userName,
			String password, String bucket) {
		MongoCredential credential = MongoCredential.createMongoCRCredential(
				userName, database, password.toCharArray());
		mongoClient = new MongoClient(DSSHelper.Str2SAList(addr),
				Arrays.asList(credential));
		db = mongoClient.getDB(database);
		if (null != bucket && !"".equals(bucket.trim())) {
			this.bucket = bucket;
		}
	}

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

	@Override
	public long getFileSize(String id) {
		GridFS fs = new GridFS(db, bucket);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(id));
		if (dbFile != null) {
			return dbFile.getLength();
		}
		return 0;
	}

	@Override
	public boolean isFileExist(String id) {
		GridFS fs = new GridFS(db, bucket);
		GridFSDBFile dbFile = fs.findOne(new ObjectId(id));
		if (dbFile != null) {
			return true;
		}
		return false;
	}

}
