package test.com.ai.paas.ipaas.dss.dssclient;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;
import com.google.gson.Gson;

import test.com.ai.paas.ipaas.dss.dssclient.base.DSSClient;

public class DocQueryTest extends DSSClient {
	private IDSSClient iDSSClient = null;
	private String id = null;
	private String content = "this is a test!";

	@Before
	public void setUp() throws Exception {
		iDSSClient = super.getClient();
		// 准备些数据
		iDSSClient.deleteAll();
		id = iDSSClient.insert(content);
		String doc = "{'age':23,'title':'test is test!'}";
		iDSSClient.insertJSON(doc);
		iDSSClient.insertJSON(doc);
		iDSSClient.insertJSON(doc);
		iDSSClient.insertJSON(doc);
		iDSSClient.insertJSON(doc);
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
	public void findById() {
		Gson gson = new Gson();
		String json = iDSSClient.findById(id);
		assertEquals(content, gson.fromJson(json, Map.class).get("content"));
	}

	/*** 正常情况测试 */
	@Test
	public void findOne() {
		Map<String, Double> query = new HashMap<>();
		query.put("age", 23.0);
		Gson gson = new Gson();
		String json = iDSSClient.find(query);
		assertEquals("test is test!",
				gson.fromJson(json, Map.class).get("title"));
	}

	/*** 正常情况测试 */
	@Test
	public void find() {
		String query = "{'age':23}";
		Gson gson = new Gson();
		String json = iDSSClient.find(query);
		assertEquals(5, gson.fromJson(json, List.class).size());
	}

	/*** 正常情况测试 */
	@Test
	public void query() {
		String query = "{'age':23}";
		Gson gson = new Gson();
		String json = iDSSClient.query(query, 1, 2);
		assertEquals(2, gson.fromJson(json, List.class).size());
	}

	/*** 正常情况测试 */
	@SuppressWarnings("deprecation")
	@Test
	public void getCount() {
		String query = "{'age':23}";
		assertEquals(5, iDSSClient.getCount(query));
	}

	/*** 空测试 */
	@Test(expected = Exception.class)
	public void findByIdBlank() {
		iDSSClient.findById("");
	}

	/*** 空测试 */
	@Test(expected = Exception.class)
	public void findOneBlank() {
		Map<String, Double> query = new HashMap<>();
		iDSSClient.find(query);
	}

	/*** 空测试 */
	@Test(expected = Exception.class)
	public void findBlank() {
		iDSSClient.find("");
	}

	/*** 空测试 */
	@Test(expected = Exception.class)
	public void queryBlank() {
		iDSSClient.query("", 1, 2);
	}

	/*** 空测试 */
	@SuppressWarnings("deprecation")
	@Test(expected = Exception.class)
	public void getCountBlank() {
		String query = "";
		iDSSClient.getCount(query);
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void findByIdNull() {
		iDSSClient.findById(null);
	}


	/*** null测试 */
	@Test(expected = Exception.class)
	public void queryNull() {
		iDSSClient.query(null, 1, 2);
	}

	/*** null测试 */
	@SuppressWarnings("deprecation")
	@Test(expected = Exception.class)
	public void getCountNull() {
		iDSSClient.getCount(null);
	}

}
