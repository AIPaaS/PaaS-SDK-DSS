package test.com.ai.paas.ipaas.dss.dssclient;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;

import test.com.ai.paas.ipaas.dss.dssclient.base.DSSClient;

public class DocDeleteTest extends DSSClient {
	private IDSSClient iDSSClient = null;
	private String id = null;
	private String id1 = null;

	@Before
	public void setUp() throws Exception {
		iDSSClient = super.getClient();
		// 准备些数据
		iDSSClient.deleteAll();
		id = iDSSClient.insert("this is a test!");
		String doc = "{'age':23,'title':'test is test!'}";
		id1 = iDSSClient.insertJSON(doc);
		Map<String, String> docM = new HashMap<>();
		docM.put("age", "24");
		docM.put("author", "xiaoming");
		iDSSClient.insert(docM);
	}

	@After
	public void destroy() throws Exception {
		iDSSClient.deleteAll();
	}

	/*** 正常情况测试 */
	@Test
	public void deleteById() {
		assertEquals(1, iDSSClient.deleteById(id));
	}

	/*** 正常情况测试 */
	@Test
	public void deleteByJson() {
		String doc = "{'age':23}";
		assertEquals(1, iDSSClient.deleteByJson(doc));
	}

	/*** 正常情况测试 */
	@Test
	public void deleteByMap() {
		Map<String, String> doc = new HashMap<>();
		doc.put("age", "24");
		assertEquals(1, iDSSClient.deleteByMap(doc));
	}

	/*** 正常情况测试 */
	@Test
	public void deleteAll() {
		assertEquals(3, iDSSClient.deleteAll());
	}

	/*** 正常情况测试 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void deleteBatch() {
		List docs = new ArrayList<>();
		Map<String, ObjectId> doc = new HashMap<>();
		doc.put("_id", new ObjectId(id));
		docs.add(doc);
		doc = new HashMap<>();
		doc.put("_id", new ObjectId(id1));
		docs.add(doc);
		assertEquals(2, iDSSClient.deleteBatch(docs));
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void deleteByIdNull() {
		iDSSClient.deleteById(null);
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void deleteByJsonNull() {
		iDSSClient.deleteByJson(null);
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void deleteByMapNull() {
		iDSSClient.deleteByMap(null);
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void deleteBatchNull() {
		iDSSClient.deleteBatch(null);
	}

	/*** 空测试 */
	@Test(expected = Exception.class)
	public void deleteByIdBlank() {
		String content = "";
		iDSSClient.deleteById(content);
	}

	/*** 空测试 */
	@Test(expected = Exception.class)
	public void deleteByJsonBlank() {
		String doc = "";
		iDSSClient.deleteByJson(doc);
	}

	/*** 空测试 */
	@Test(expected = Exception.class)
	public void deleteByMapBlank() {
		Map<String, String> doc = new HashMap<>();
		iDSSClient.deleteByMap(doc);
	}

	/*** 空测试 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test(expected = Exception.class)
	public void deleteBatchBlank() {
		List docs = new ArrayList<>();
		iDSSClient.deleteBatch(docs);
	}

}
