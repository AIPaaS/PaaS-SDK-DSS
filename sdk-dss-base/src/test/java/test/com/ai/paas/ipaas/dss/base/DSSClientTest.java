package test.com.ai.paas.ipaas.dss.base;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ai.paas.ipaas.dss.base.DSSBaseFactory;
import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class DSSClientTest {
	private static IDSSClient dssClient = null;
	private Gson gson = new Gson();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		dssClient = DSSBaseFactory.getClient(
				"{\"mongoServer\":\"10.19.10.84:37017\",\"database\":\"dsswo\",\"userName\":\"dsswouser\",\"password\":\"dsswopwd\",\"bucket\":\"dsswofs\"}");

	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		if (null != dssClient)
			dssClient.close();
	}

	@Before
	public void setUp() throws Exception {

	}

	@After
	public void tearDown() throws Exception {
		dssClient.dropAllIndex();
	}

	@Test
	public void testSaveFile() {
		Long startTime = System.currentTimeMillis();
		System.out.println("开始时间戳：" + startTime + "毫秒");
		File file = new File("/Users/douxiaofeng/Downloads/a.rar");
		String fileid = dssClient.save(file, "remark");
		Long endTime = System.currentTimeMillis();
		Long costTime = endTime - startTime;
		System.out.println("文件上传成功，fileid=" + fileid + ", 结束时间戳：" + endTime + "， 用时" + costTime + "毫秒");
	}

	@Test
	public void testSaveFileByChunk() {
		Long startTime = System.currentTimeMillis();
		System.out.println("开始时间戳：" + startTime + "毫秒");
		File file = new File("/Users/douxiaofeng/Downloads/a.rar");
		String fileid = dssClient.save(file, "remark", 8192 * 16);
		Long endTime = System.currentTimeMillis();
		Long costTime = endTime - startTime;
		System.out.println("文件上传成功，fileid=" + fileid + ", 结束时间戳：" + endTime + "， 用时" + costTime + "毫秒");
	}

	@Test
	public void testSaveFileString() {
		// 先创建一个文件
		OutputStream out = null;
		File test = null;
		try {
			test = new File("test.txt");
			out = new FileOutputStream(test);
			String cnt = "this is a test for file!";
			out.write(cnt.getBytes());
			out.flush();
			// 开始插入
			String id = dssClient.save(test, "test");
			byte[] bytes = dssClient.read(id);
			assertEquals(cnt, new String(bytes));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != out) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (null != test)
				test.deleteOnExit();
		}
	}

	@Test
	public void testSaveByteArrayString() {
		String cnt = "this is a test for byte!";
		// 开始插入
		String id = dssClient.save(cnt.getBytes(), "test");
		byte[] bytes = dssClient.read(id);
		assertEquals(cnt, new String(bytes));
	}

	@Test
	public void testRead() {
		String cnt = "this is a test for read!";
		// 开始插入
		String id = dssClient.save(cnt.getBytes(), "test");
		byte[] bytes = dssClient.read(id);
		assertEquals(cnt, new String(bytes));
	}

	@Test
	public void testReadById() {
		Long startTime = System.currentTimeMillis();
		String id = "5acb543b263c149b9274bc1b";
		// 开始插入
		byte[] bytes = dssClient.read(id);
		Long endTime = System.currentTimeMillis();
		Long costTime = endTime - startTime;
		System.out.println("文件成功， 结束时间戳：" + endTime + "， 用时" + costTime + "毫秒");
	}

	@Test
	public void testReadByIdToFile() {
		Long startTime = System.currentTimeMillis();
		String id = "5acb543b263c149b9274bc1b";
		// 开始插入
		dssClient.readToFile(id, "/Users/douxiaofeng/Downloads/b.rar");
		Long endTime = System.currentTimeMillis();
		Long costTime = endTime - startTime;
		System.out.println("文件成功， 结束时间戳：" + endTime + "， 用时" + costTime + "毫秒");
	}

	@Test
	public void testReadByIdToOut() throws Exception {
		Long startTime = System.currentTimeMillis();
		String id = "5acb543b263c149b9274bc1b";
		// 开始插入
		OutputStream out = new FileOutputStream(new File("/Users/douxiaofeng/Downloads/b.rar"));
		dssClient.readToFile(id, out);
		out.close();
		Long endTime = System.currentTimeMillis();
		Long costTime = endTime - startTime;
		System.out.println("文件成功， 结束时间戳：" + endTime + "， 用时" + costTime + "毫秒");
	}

	@Test
	public void testDelete() {
		String cnt = "this is a test for delete!";
		// 开始插入
		String id = dssClient.save(cnt.getBytes(), "test");
		assertTrue(dssClient.delete(id));
	}

	@Test
	public void testUpdateStringByteArray() {

		// 先创建一个文件
		String cnt = "this is a test for update!";
		// 开始插入
		String id = dssClient.save(cnt.getBytes(), "test");
		cnt = "this is a test for update new!";
		id = dssClient.update(id, cnt.getBytes());
		byte[] bytes = dssClient.read(id);
		assertEquals(cnt, new String(bytes));
	}

	@Test
	public void testGetLastUpdateTime() {
		// 先创建一个文件
		String cnt = "this is a test for update!";
		// 开始插入
		String id = dssClient.save(cnt.getBytes(), "test");
		Date updateTime = dssClient.getLastUpdateTime(id);
		System.out.println(updateTime);
		assertNotNull(updateTime);
	}

	@Test
	public void testUpdateStringFile() {
		// 先创建一个文件
		OutputStream out = null;
		File test = null;
		try {
			test = new File("test.txt");
			out = new FileOutputStream(test);
			String cnt = "this is a test for update!";
			out.write(cnt.getBytes());
			out.flush();
			// 开始插入
			String id = dssClient.save(test, "test");
			out.write("".getBytes());
			out.flush();
			out.close();
			out = new FileOutputStream(test);
			cnt = "this is a test for update new!";
			out.write(cnt.getBytes());
			out.flush();
			id = dssClient.update(id, test);
			byte[] bytes = dssClient.read(id);
			assertEquals(cnt, new String(bytes));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != out) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (null != test)
				test.deleteOnExit();

		}
	}

	@Test
	public void testGetFileSize() {
		String cnt = "this is a test for file size!";
		// 开始插入
		String id = dssClient.save(cnt.getBytes(), "test");
		long size = dssClient.getFileSize(id);
		assertEquals(cnt.getBytes().length, size);
	}

	@Test
	public void testIsFileExist() {
		String cnt = "this is a test for file size!";
		// 开始插入
		String id = dssClient.save(cnt.getBytes(), "test");
		assertTrue(dssClient.isFileExist(id));
	}

	@Test
	public void testInsertString() {
		String cnt = "this is a test for insert plain string !";
		String id = dssClient.insert(cnt);
		String doc = dssClient.findById(id);
		JsonObject json = gson.fromJson(doc, JsonObject.class);
		assertEquals(cnt, json.get("content").getAsString());
	}

	@Test
	public void testInsertJSON() {
		JsonObject json = new JsonObject();
		json.addProperty("name", "cmc-asc");
		json.addProperty("age", 1489716180934L);
		json.addProperty("sex", "Male");
		String id = dssClient.insertJSON(gson.toJson(json));
		String doc = dssClient.findById(id);
		System.out.println("--" + doc);
		json = gson.fromJson(doc, JsonObject.class);
		assertEquals("cmc-asc", json.get("name").getAsString());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testInsertMap() {
		Map json = new HashMap();
		json.put("name", "cmc-asc");
		json.put("age", 21);
		json.put("sex", "Male");
		String id = dssClient.insert(json);
		String doc = dssClient.findById(id);
		System.out.println(doc);
		JsonObject jso = gson.fromJson(doc, JsonObject.class);
		assertEquals("cmc-asc", jso.get("name").getAsString());
	}

	@Test
	public void testInsertBatch() {
		dssClient.deleteAll();
		List<Map<String, Object>> docs = new ArrayList<>();
		for (int i = 0; i < 1000; i++) {
			Map<String, Object> doc = new HashMap<>();
			doc.put("name", "test");
			doc.put("age", 21);
			doc.put("sex", "Male");
			docs.add(doc);
		}
		dssClient.insertBatch(docs);
		assertEquals(1000, dssClient.count("{'name':'test'}"));
	}

	@Test
	public void testDeleteById() {
		String id = dssClient.insert("This is a test for delete!");
		assertEquals(1, dssClient.deleteById(id));
	}

	@Test
	public void testDeleteByJson() {
		String id = dssClient.insert("This is a test for delete!");
		assertEquals(1, dssClient.deleteByJson("{'_id':'" + id + "'}"));
	}

	@Test
	public void testDeleteByMap() {
		String id = dssClient.insert("This is a test for delete!");
		Map<String, Object> map = new HashMap<>();
		map.put("_id", id);
		assertEquals(1, dssClient.deleteByMap(map));
	}

	@Test
	public void testDeleteAll() {
		dssClient.deleteAll();
		dssClient.insert("This is a test for delete all!");
		dssClient.insert("This is a test for delete all!");
		dssClient.insert("This is a test for delete all!");
		assertEquals(3, dssClient.count("{}"));
		dssClient.deleteAll();
		assertEquals(0, dssClient.count("{}"));
	}

	@Test
	public void testDeleteBatch() {
		dssClient.deleteAll();
		dssClient.insertJSON("{'name':'张三','age':21}");
		dssClient.insertJSON("{'name':'李四','age':22}");
		dssClient.insertJSON("{'name':'王五','age':23}");
		Map<String, Object> doc1 = new HashMap<>();
		doc1.put("name", "张三");
		Map<String, Object> doc2 = new HashMap<>();
		doc2.put("name", "李四");
		List<Map<String, Object>> docs = new ArrayList<>();
		docs.add(doc1);
		docs.add(doc2);
		assertEquals(2, dssClient.deleteBatch(docs));
		assertEquals(1, dssClient.count(""));
	}

	@Test
	public void testUpdateById() {
		dssClient.deleteAll();
		String id = dssClient.insertJSON("{'name':'张三','age':21}");
		assertEquals(1, dssClient.updateById(id, "{'age':23}"));
		JsonObject doc = gson.fromJson(dssClient.findById(id), JsonObject.class);
		assertEquals(23, doc.get("age").getAsInt());
		assertEquals("张三", doc.get("name").getAsString());
	}

	@Test
	public void testUpdateStringString() {
		dssClient.deleteAll();
		dssClient.insertJSON("{'name':'张三','age':21}");
		dssClient.insertJSON("{'name':'张三','age':22}");
		dssClient.insertJSON("{'name':'王五','age':23}");
		assertEquals(2, dssClient.update("{'name':'张三'}", "{'age':23}"));
		String docs = dssClient.find("{'name':'张三'}");
		JsonArray array = gson.fromJson(docs, JsonArray.class);
		Iterator<JsonElement> iter = array.iterator();
		while (iter.hasNext()) {
			JsonObject json = (JsonObject) iter.next();
			assertEquals("张三", json.get("name").getAsString());
			assertEquals(23, json.get("age").getAsInt());
		}
	}

	@Test
	public void testUpdateOrInsert() {
		dssClient.deleteAll();
		dssClient.insertJSON("{'name':'张三','age':21}");
		assertEquals(1, dssClient.upsert("{'name':'张三'}", "{'age':23}"));
		String docs = dssClient.find("{'name':'张三'}");
		JsonArray array = gson.fromJson(docs, JsonArray.class);
		Iterator<JsonElement> iter = array.iterator();
		assertEquals(1, array.size());
		while (iter.hasNext()) {
			JsonObject json = (JsonObject) iter.next();
			assertEquals("张三", json.get("name").getAsString());
			assertEquals(23, json.get("age").getAsInt());
		}
		assertEquals(0, dssClient.upsert("{'name':'李四'}", "{'age':24}"));
		docs = dssClient.find("{'age':24}");
		array = gson.fromJson(docs, JsonArray.class);
		iter = array.iterator();
		assertEquals(1, array.size());
		while (iter.hasNext()) {
			JsonObject json = (JsonObject) iter.next();
			assertEquals("李四", json.get("name").getAsString());
			assertEquals(24, json.get("age").getAsInt());
		}
	}

	@Test
	public void testFindById() {
		dssClient.deleteAll();
		String id = dssClient.insertJSON("{'name':'张三','age':21}");
		String document = dssClient.findById(id);
		JsonObject json = gson.fromJson(document, JsonObject.class);
		assertEquals("张三", json.get("name").getAsString());
		assertEquals(21, json.get("age").getAsInt());
	}

	@Test
	public void testFindMap() {
		dssClient.deleteAll();
		dssClient.insertJSON("{'name':'张三','age':21}");
		dssClient.insertJSON("{'name':'李四','age':21}");
		dssClient.insertJSON("{'name':'王五','age':21}");
		Map<String, Integer> map = new HashMap<>();
		map.put("age", 21);
		String documents = dssClient.find(map);
		JsonArray array = gson.fromJson(documents, JsonArray.class);
		assertEquals(3, array.size());
	}

	@Test
	public void testFind() {
		dssClient.deleteAll();
		dssClient.insertJSON("{'name':'张三','age':21}");
		dssClient.insertJSON("{'name':'李四','age':21}");
		dssClient.insertJSON("{'name':'王五','age':21}");
		String documents = dssClient.find("{'age':21}");
		JsonArray array = gson.fromJson(documents, JsonArray.class);
		assertEquals(3, array.size());
	}

	@Test
	public void testQuery() {
		dssClient.deleteAll();
		dssClient.insertJSON("{'name':'张三','age':21}");
		dssClient.insertJSON("{'name':'李四','age':21}");
		dssClient.insertJSON("{'name':'王五','age':21}");
		String documents = dssClient.query("{'age':21}", 1, 2);
		JsonArray array = gson.fromJson(documents, JsonArray.class);
		assertEquals(2, array.size());
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testGetCount() {
		dssClient.deleteAll();
		dssClient.insertJSON("{'name':'张三','age':21}");
		dssClient.insertJSON("{'name':'李四','age':21}");
		dssClient.insertJSON("{'name':'王五','age':21}");
		assertEquals(3, dssClient.getCount(""));
	}

	@Test(expected = Exception.class)
	public void testAddIndex() {
		dssClient.deleteAll();
		dssClient.dropAllIndex();
		dssClient.insertJSON("{'name':'张三','age':21}");
		dssClient.insertJSON("{'name':'李四','age':22}");
		dssClient.insertJSON("{'name':'王五','age':23}");
		dssClient.addIndex("age", true);
		dssClient.insertJSON("{'name':'王五','age':23}");
	}

	@Test(expected = Exception.class)
	public void testDropIndex() {
		dssClient.deleteAll();
		if (dssClient.isIndexExist("age"))
			dssClient.dropIndex("age");
		dssClient.insertJSON("{'name':'张三','age':21}");
		dssClient.insertJSON("{'name':'李四','age':22}");
		dssClient.insertJSON("{'name':'王五','age':23}");
		dssClient.addIndex("age", true);
		dssClient.insertJSON("{'name':'王五','age':23}");
		// 下面不执行
		dssClient.dropIndex("age");
		dssClient.insertJSON("{'name':'王五','age':23}");
	}

	@Test
	public void testIsIndexExist() {
		dssClient.deleteAll();
		dssClient.dropAllIndex();
		assertFalse(dssClient.isIndexExist("age"));
		dssClient.insertJSON("{'name':'张三','age':21}");
		dssClient.insertJSON("{'name':'李四','age':22}");
		dssClient.insertJSON("{'name':'王五','age':23}");
		dssClient.addIndex("age", true);
		assertTrue(dssClient.isIndexExist("age"));
	}

	@Test
	public void testGetSize() {
		dssClient.deleteAll();
		dssClient.dropAllIndex();
		assertFalse(dssClient.isIndexExist("age"));
		dssClient.insertJSON("{'name':'张三','age':21}");
		dssClient.insertJSON("{'name':'李四','age':22}");
		dssClient.insertJSON("{'name':'王五','age':23}");
		System.out.print(dssClient.getSize());
		assertTrue(dssClient.getSize() > 0);
	}

}
