package com.ai.paas.ipaas.dss.base.impl;

import static com.mongodb.client.model.Filters.eq;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;

import com.ai.paas.ipaas.dss.base.exception.DSSRuntimeException;
import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;
import com.ai.paas.ipaas.util.Assert;
import com.ai.paas.ipaas.util.StringUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.PolygonCoordinates;
import com.mongodb.client.model.geojson.Position;

public class DSSClient implements IDSSClient {

	private static final Logger log = LogManager.getLogger(DSSClient.class);
	private static final String FILE_NAME = "filename";

	private final static String REMARK = "remark";
	private MongoClient mongoClient;
	private MongoDatabase db;
	private String defaultCollection = null;
	private final int MAX_QUERY_SIZE = 1000;
	Gson gson = new GsonBuilder().registerTypeAdapter(ObjectId.class, new ObjectIdTypeAdapter()).create();

	public DSSClient(String addr, String database, String userName, String password, String bucket) {
		MongoClientOptions.Builder builder = new MongoClientOptions.Builder();

		// build the connection options
		builder.maxConnectionIdleTime(60000);// set the max wait time in (ms)
		MongoClientOptions opts = builder.build();
		MongoCredential credential = MongoCredential.createCredential(userName, database, password.toCharArray());
		mongoClient = new MongoClient(DSSHelper.Str2SAList(addr), Arrays.asList(credential), opts);
		db = mongoClient.getDatabase(database);
		// 默认表就是服务标识
		defaultCollection = bucket;
	}

	@Override
	public String save(File file, String remark) {
		return save(file, remark, 358400);
	}

	@Override
	public String save(File file, String remark, int chunkSize) {
		Assert.notNull(file, "The insert file is null!");
		String fileType = DSSHelper.getFileType(file.getName());
		GridFSBucket gridBucket = GridFSBuckets.create(db);
		ObjectId fileId = null;
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(file);
			GridFSUploadOptions uploadOptions = new GridFSUploadOptions().chunkSizeBytes(chunkSize)
					.metadata(new Document("type", fileType).append(REMARK, remark).append(FILE_NAME, file.getName()));
			fileId = gridBucket.uploadFromStream(file.getName(), inputStream, uploadOptions);
			return fileId.toString();
		} catch (Exception e) {
			log.error("", e);
			throw new DSSRuntimeException(e);
		} finally {
			if (null != inputStream) {
				try {
					inputStream.close();
				} catch (IOException e) {
					log.error("", e);
				}
			}
		}
	}

	@Override
	public String save(byte[] bytes, String remark) {
		return save(bytes, remark, 358400);
	}

	@Override
	public String save(byte[] bytes, String remark, int chunkSize) {
		if (bytes == null || bytes.length <= 0) {
			throw new DSSRuntimeException(new Exception("bytes illegal"));
		}
		GridFSBucket gridBucket = GridFSBuckets.create(db);
		ObjectId fileId = null;
		InputStream inputStream = null;
		try {
			inputStream = new ByteArrayInputStream(bytes);
			GridFSUploadOptions uploadOptions = new GridFSUploadOptions().chunkSizeBytes(chunkSize)
					.metadata(new Document("remark", remark));
			fileId = gridBucket.uploadFromStream("", inputStream, uploadOptions);
			return fileId.toString();
		} catch (Exception e) {
			log.error(e.toString());
			throw new DSSRuntimeException(e);
		} finally {
			if (null != inputStream) {
				try {
					inputStream.close();
				} catch (IOException e) {
					log.error("", e);
				}
			}
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
		GridFSBucket gridBucket = GridFSBuckets.create(db);
		GridFSDownloadStream stream = null;
		try {
			stream = gridBucket.openDownloadStream(new ObjectId(id));
			int fileLength = (int) stream.getGridFSFile().getLength();
			byte[] buffer = new byte[fileLength];
			stream.read(buffer);
			return buffer;
		} catch (Exception e) {
			log.error(e.toString());
			throw new DSSRuntimeException(e);
		} finally {
			if (null != stream) {
				stream.close();
			}
		}
	}

	@Override
	public void readToFile(String id, String fileName) {
		if (id == null || "".equals(id)) {
			log.error("id illegal");
			throw new DSSRuntimeException(new Exception("id illegal"));
		}
		if (fileName == null || "".equals(fileName)) {
			log.error("fileName illegal");
			throw new DSSRuntimeException(new Exception("fileName illegal"));
		}
		GridFSBucket gridBucket = GridFSBuckets.create(db);
		FileOutputStream streamToDownloadTo = null;
		try {
			streamToDownloadTo = new FileOutputStream(fileName);
			gridBucket.downloadToStream(new ObjectId(id), streamToDownloadTo);
		} catch (Exception e) {
			log.error(e.toString());
			throw new DSSRuntimeException(e);
		} finally {
			if (null != streamToDownloadTo) {
				try {
					streamToDownloadTo.close();
				} catch (IOException e) {
					log.error("", e);
				}
			}
		}
	}

	public void readToFile(String id, OutputStream out) {
		if (id == null || "".equals(id)) {
			log.error("id illegal");
			throw new DSSRuntimeException(new Exception("id illegal"));
		}
		if (out == null) {
			log.error("out illegal");
			throw new DSSRuntimeException(new Exception("out illegal"));
		}
		GridFSBucket gridBucket = GridFSBuckets.create(db);
		try {
			gridBucket.downloadToStream(new ObjectId(id), out);
		} catch (Exception e) {
			log.error(e.toString());
			throw new DSSRuntimeException(e);
		}
	}

	public byte[] readByName(String fileName) {
		if (fileName == null || "".equals(fileName)) {
			log.error("fileName illegal");
			throw new DSSRuntimeException(new Exception("fileName illegal"));
		}
		GridFSBucket gridBucket = GridFSBuckets.create(db);
		GridFSDownloadStream stream = null;
		try {
			stream = gridBucket.openDownloadStream(fileName);
			int fileLength = (int) stream.getGridFSFile().getLength();
			byte[] buffer = new byte[fileLength];
			stream.read(buffer);
			return buffer;
		} catch (Exception e) {
			log.error(e.toString());
			throw new DSSRuntimeException(e);
		} finally {
			if (null != stream) {
				stream.close();
			}
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
		try {
			GridFSBucket gridBucket = GridFSBuckets.create(db);
			gridBucket.delete(new ObjectId(id));
			return true;
		} catch (Exception e) {
			log.error("delete id: " + id, e);
			return false;
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#update(java.lang.String,
	 * byte[])
	 */
	@Override
	public String update(String id, byte[] bytes) {
		if (bytes == null || id == null || "".equals(id)) {
			log.error("id or bytes illegal");
			throw new DSSRuntimeException(new Exception("id or bytes illegal"));
		}
		delete(id);
		return save(bytes, "");
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
		GridFSBucket gridBucket = GridFSBuckets.create(db);
		GridFSFindIterable files = gridBucket.find(eq("_id", new ObjectId(id)));
		if (files == null || null == files.first()) {
			log.error("file missing");
			throw new DSSRuntimeException(new Exception("file missing"));
		}
		GridFSFile gridFSFile = files.first();
		return gridFSFile.getUploadDate();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#update(java.lang.String,
	 * java.io.File)
	 */
	@Override
	public String update(String id, File file) {
		if (file == null || id == null || "".equals(id)) {
			log.error("id or file illegal");
			throw new DSSRuntimeException(new Exception("id or file illegal"));
		}
		delete(id);
		return save(file, "");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#getFileSize(java.lang.String)
	 */
	@Override
	public long getFileSize(String id) {
		GridFSBucket gridBucket = GridFSBuckets.create(db);
		GridFSFindIterable files = gridBucket.find(eq("_id", new ObjectId(id)));
		if (files == null || null == files.first()) {
			return 0;
		}
		return files.first().getLength();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#isFileExist(java.lang.String)
	 */
	@Override
	public boolean isFileExist(String id) {
		GridFSBucket gridBucket = GridFSBuckets.create(db);
		GridFSFindIterable files = gridBucket.find(eq("_id", new ObjectId(id)));
		if (files == null || null == files.first()) {
			return false;
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#insert(java.lang.String)
	 */
	@Override
	public String insert(String content) {
		Document doc = new Document();
		ObjectId id = new ObjectId();
		doc.put("_id", id);
		doc.put("content", content);
		db.getCollection(defaultCollection).insertOne(doc);
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
		Document dbObj = Document.parse(doc);
		ObjectId id = new ObjectId();
		dbObj.put("_id", id);
		db.getCollection(defaultCollection).insertOne(dbObj);
		return id.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#insert(java.util.Map)
	 */
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public String insert(Map doc) {
		if (null == doc || doc.size() <= 0)
			throw new IllegalArgumentException();
		Document dbObj = new Document(doc);
		ObjectId id = new ObjectId();
		dbObj.put("_id", id);
		db.getCollection(defaultCollection).insertOne(dbObj);
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
		List<Document> documents = new ArrayList<Document>();
		for (int i = 0; i < docs.size(); i++) {
			Document dbObj = new Document(docs.get(i));
			documents.add(dbObj);
		}
		db.getCollection(defaultCollection).insertMany(documents);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#deleteById(java.lang.String)
	 */
	@Override
	public long deleteById(String id) {
		return db.getCollection(defaultCollection).deleteOne(eq("_id", new ObjectId(id))).getDeletedCount();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#deleteByJson(java.lang.String)
	 */
	@Override
	public long deleteByJson(String doc) {
		Document dbObj = Document.parse(doc);
		if (dbObj.containsKey("_id")) {
			String id = dbObj.getString("_id");
			dbObj.remove("_id");
			dbObj.append("_id", new ObjectId(id));
		}
		return db.getCollection(defaultCollection).deleteMany(dbObj).getDeletedCount();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#deleteByMap(java.util.Map)
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public long deleteByMap(Map doc) {
		if (null == doc || doc.size() <= 0)
			throw new IllegalArgumentException();
		@SuppressWarnings("unchecked")
		Document dbObj = new Document(doc);
		if (dbObj.containsKey("_id")) {
			String id = dbObj.getString("_id");
			dbObj.remove("_id");
			dbObj.append("_id", new ObjectId(id));
		}
		return db.getCollection(defaultCollection).deleteMany(dbObj).getDeletedCount();
	}

	public boolean collectionExists(final String collectionName) {
		if (StringUtil.isBlank(collectionName)) {
			return false;
		}

		final MongoIterable<String> iterable = db.listCollectionNames();
		try (final MongoCursor<String> it = iterable.iterator()) {
			while (it.hasNext()) {
				if (it.next().equalsIgnoreCase(collectionName)) {
					return true;
				}
			}
		}

		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#deleteAll()
	 */
	@Override
	public long deleteAll() {
		if (collectionExists(defaultCollection)) {
			// 慎重使用，不可恢复
			Document dbObj = new Document();
			return db.getCollection(defaultCollection).deleteMany(dbObj).getDeletedCount();
		}
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#deleteBatch(java.util.List)
	 */
	@Override
	public long deleteBatch(List<Map<String, Object>> docs) {
		if (docs == null || docs.isEmpty()) {
			throw new IllegalArgumentException();
		}
		int total = 0;
		for (int i = 0; i < docs.size(); i++) {
			Document dbObj = new Document(docs.get(i));
			total += db.getCollection(defaultCollection).deleteMany(dbObj).getDeletedCount();
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
	public long updateById(String id, String doc) {
		Document dbObj = Document.parse(doc);
		Document modifiedObject = new Document("$set", dbObj);
		return db.getCollection(defaultCollection).updateOne(eq("_id", new ObjectId(id)), modifiedObject)
				.getModifiedCount();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#update(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public long update(String query, String doc) {
		Document qryObj = Document.parse(query);
		Document dbObj = Document.parse(doc);
		Document modifiedObject = new Document("$set", dbObj);
		return db.getCollection(defaultCollection).updateMany(qryObj, modifiedObject).getModifiedCount();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#updateOrInsert(java.lang.
	 * String , java.lang.String)
	 */
	@Override
	public long upsert(String query, String doc) {
		Document qryObj = Document.parse(query);
		Document dbObj = Document.parse(doc);
		Document modifiedObject = new Document("$set", dbObj);
		UpdateOptions options = new UpdateOptions().upsert(true);
		return db.getCollection(defaultCollection).updateMany(qryObj, modifiedObject, options).getModifiedCount();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#findById(java.lang.String)
	 */
	@Override
	public String findById(String id) {
		FindIterable<Document> docs = db.getCollection(defaultCollection).find(eq("_id", new ObjectId(id)));
		if (docs == null || null == docs.first()) {
			return null;
		}
		return gson.toJson(docs.first());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.ai.paas.ipaas.dss.base.impl.IDSSClient#findOne(java.util.Map)
	 */
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public String find(Map doc) {
		Document query = new Document(doc);
		List<Document> documents = db.getCollection(defaultCollection).find(query).limit(MAX_QUERY_SIZE)
				.into(new ArrayList<Document>());
		if (null != documents)
			return gson.toJson(documents);
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
		if (StringUtil.isBlank(query))
			query = "{}";
		Document qryObj = Document.parse(query);
		List<Document> documents = (List<Document>) db.getCollection(defaultCollection).find(qryObj)
				.limit(MAX_QUERY_SIZE).into(new ArrayList<Document>());
		if (null != documents) {
			return gson.toJson(documents);
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
		Document qryObj = Document.parse(query);
		List<Document> documents = db.getCollection(defaultCollection).find(qryObj)
				.skip((pageNumber >= 1 ? (pageNumber - 1) * pageSize : 0)).limit(pageSize)
				.into(new ArrayList<Document>());
		if (null != documents) {
			return gson.toJson(documents);
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
		if (StringUtil.isBlank(query))
			query = "{}";
		Document qryObj = Document.parse(query);
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
		IndexOptions options = new IndexOptions();

		// ensure the index is unique
		options.unique(true);
		BasicDBObject dbo = new BasicDBObject(field, 1);
		String idx = db.getCollection(defaultCollection).createIndex(dbo, options);
		log.info("Index on field:" + field + "created! name:" + idx);
	}

	@Override
	public void dropAllIndex() {
		db.getCollection(defaultCollection).dropIndexes();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.ai.paas.ipaas.dss.base.impl.IDSSClient#dropIndex(java.lang.String)
	 */
	@Override
	public void dropIndex(String field) {
		BasicDBObject dbo = new BasicDBObject(field, 1);
		db.getCollection(defaultCollection).dropIndex(dbo);
	}

	public boolean isIndexExist(String field) {
		String indexName = field;
		List<Document> indexs = db.getCollection(defaultCollection).listIndexes().into(new ArrayList<Document>());
		if (null == indexs || indexs.size() <= 0)
			return false;
		else {
			boolean found = false;
			for (Document dbo : indexs) {
				if (dbo.get("name").toString().indexOf(indexName) >= 0) {
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
		Document tableResult = db.runCommand(new Document("collStats", defaultCollection));
		Document fileResult = db.runCommand(new Document("collStats", "fs.chunks"));
		int dataSize = 0;
		int fileSize = 0;
		if (null != tableResult) {
			if (null != tableResult.get("size"))
				dataSize = tableResult.getInteger("size");
		}
		if (null != fileResult) {
			if (null != fileResult.get("size"))
				fileSize = fileResult.getInteger("size");
		}
		size = Math.round(dataSize + fileSize);
		return size;
	}

	public static void main(String[] args) {
		IDSSClient dss = new DSSClient("10.1.234.150:37017", "dss001", "dss001user", "dss001pwd", "gisfs");
		// dss.createGeoIndex("geom");
		// System.out
		// .println(dss.withinPolygon("geom", "5ab1ffb696ca0318249ef4d8", new
		// double[] { 117.215762, 39.134247 }));
		// final BasicDBObject geo = new BasicDBObject("geom", new Point((new
		// Position(1.5d,0.5d))));
		// String polygonId = dss.insertJSON(geo.toJson());
		List<double[]> values = new ArrayList<>();
		values.add(new double[] { 1.0, 1.0 });
		values.add(new double[] { 2.0, 1.0 });
		values.add(new double[] { 2.0, 0.0 });
		values.add(new double[] { 1.0, 0.0 });
		values.add(new double[] { 1.0, 1.0 });
		// System.out.println(dss.withinPolygon("geom", values, new double[] {
		// 1.5, 0.5 }));
		System.out.println(dss.findGeoWithinPolygon("geom", values));
	}

	@Override
	public void close() {
		if (null != mongoClient)
			mongoClient.close();
	}

	@Override
	public long count(String query) {
		if (StringUtil.isBlank(query))
			query = "{}";
		Document qryObj = Document.parse(query);
		return db.getCollection(defaultCollection).count(qryObj);
	}

	private class ObjectIdTypeAdapter extends TypeAdapter<ObjectId> {
		@Override
		public void write(final JsonWriter out, final ObjectId value) throws IOException {
			out.beginObject().name("$oid").value(value.toString()).endObject();
		}

		@Override
		public ObjectId read(final JsonReader in) throws IOException {
			in.beginObject();
			assert "$oid".equals(in.nextName());
			String objectId = in.nextString();
			in.endObject();
			return new ObjectId(objectId);
		}
	}

	@Override
	public boolean withinPolygon(final String field, final String mongoId, final double[] point) {
		// db.geom.find({polygons:
		// {$geoIntersects:
		// {$geometry:{ "type" : "Point",
		// "coordinates" : [ 17.3734, 78.4738 ] }
		// }
		// }
		// });
		Assert.notNull(field, "the geo field can not be null!");
		Assert.notNull(mongoId, "the geo record primary id can not be null!");
		Assert.notNull(point, "the point can not be null!");
		final BasicDBObject pLocation = new BasicDBObject("type", "Point");
		pLocation.put("coordinates", point);
		final BasicDBObject geometry = new BasicDBObject("$geometry", pLocation);
		final BasicDBObject filter = new BasicDBObject("$geoIntersects", geometry);
		final BasicDBObject query = new BasicDBObject(field, filter);
		log.info("the geo withinPolygon query:" + query);
		List<Document> documents = db.getCollection(defaultCollection).find(query).into(new ArrayList<Document>());
		log.info("the geo withinPolygon query result:" + documents);
		if (null == documents)
			// 不在任何区域内
			return false;

		boolean found = false;
		for (Document document : documents) {
			Object id = document.get("_id");
			if (null != id && id.toString().equalsIgnoreCase(mongoId)) {
				found = true;
				break;
			}
		}
		// 可能在别的区域内
		return found;
	}

	@Override
	public boolean withinPolygon(final String field, final List<double[]> coordinates, final double[] point) {
		Assert.notNull(field, "the geo field can not be null!");
		Assert.notNull(coordinates, "the geo record json can not be null!");
		Assert.notNull(point, "the point can not be null!");
		// 准备数据
		List<Position> values = new ArrayList<>();
		for (double[] coordinate : coordinates) {
			values.add(new Position(coordinate[0], coordinate[1]));
		}
		@SuppressWarnings("unchecked")
		PolygonCoordinates polygonCoordinates = new PolygonCoordinates(values);
		Polygon polygon = new Polygon(polygonCoordinates);
		final BasicDBObject geo = new BasicDBObject(field, polygon);
		String polygonId = insertJSON(geo.toJson());
		boolean found = withinPolygon(field, polygonId, point);
		deleteById(polygonId);
		return found;
	}

	@Override
	public String findGeoWithinPolygon(String field, List<double[]> coordinates) {
		Assert.notNull(field, "the geo field can not be null!");
		Assert.notNull(coordinates, "the geo record json can not be null!");
		// 准备数据
		List<List<double[]>> values = new ArrayList<>();
		values.add(coordinates);
		BasicDBObject polygon = new BasicDBObject("type", "Polygon");
		polygon.put("coordinates", values);
		final BasicDBObject query = new BasicDBObject(field,
				new BasicDBObject("$geoWithin", new BasicDBObject("$geometry", polygon)));
		log.info("the geo within query:" + query);
		List<Document> documents = db.getCollection(defaultCollection).find(query).into(new ArrayList<Document>());
		log.info("the geo within query result:" + documents);
		if (null == documents)
			return null;
		return gson.toJson(documents);
	}

	@Override
	public void createGeoIndex(String field) {
		db.getCollection(defaultCollection).createIndex(Indexes.geo2dsphere(field));
	}
}
