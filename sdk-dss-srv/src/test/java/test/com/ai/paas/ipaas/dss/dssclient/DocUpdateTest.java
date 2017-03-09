package test.com.ai.paas.ipaas.dss.dssclient;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.com.ai.paas.ipaas.dss.dssclient.base.DSSClient;

import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;

public class DocUpdateTest extends DSSClient {
	private IDSSClient iDSSClient = null;
	private String id = null;

	@Before
	public void setUp() throws Exception {
		iDSSClient = super.getClient();
		// 准备些数据
		iDSSClient.deleteAll();
		id = iDSSClient.insert("this is a test!");
		String doc = "{'age':23,'title':'test is test!'}";
		iDSSClient.insertJSON(doc);
		Map<String, Object> docM = new HashMap<>();
		docM.put("age", Double.valueOf("23"));
		docM.put("author", "xiaoming");
		iDSSClient.insert(docM);
	}

	@After
	public void destroy() throws Exception {
		 iDSSClient.deleteAll();
	}

	/*** 正常情况测试 */
	@Test
	public void updateById() {
		String doc = "{'age':23,'title':'test is test!'}";
		assertEquals(1, iDSSClient.updateById(id, doc));
	}

	/*** 正常情况测试 */
	@Test
	public void update() {
		String query = "{'age':23}";
		String doc = "{'age':25,'url':'xxxxxx'}";
		assertEquals(2, iDSSClient.update(query, doc));
	}

	/*** 正常情况测试 */
	@Test
	public void updateOrInsert() {
		String query = "{'age':23}";
		String doc = "{'age':25,'url':'xxxxxx'}";
		assertEquals(2, iDSSClient.upsert(query, doc));
		query = "{'age':25}";
		doc = "{'age':26,'url':'xxxxxx111'}";
		assertEquals(2, iDSSClient.upsert(query, doc));
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void updateByIdNull() {
		String doc = null;
		iDSSClient.updateById(id, doc);
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void updateNull() {
		String query = null;
		String doc = null;
		iDSSClient.update(query, doc);
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void updateOrInsertNull() {
		String query = null;
		String doc = null;
		iDSSClient.upsert(query, doc);
	}

	/*** 空测试 */
	@Test(expected = Exception.class)
	public void updateByIdBlank() {
		String doc = "";
		iDSSClient.updateById(id, doc);
	}

	/*** 空测试 */
	@Test(expected = Exception.class)
	public void updateBlank() {
		String query = "";
		String doc = "";
		iDSSClient.update(query, doc);
	}

	/*** 空测试 */
	@Test(expected = Exception.class)
	public void updateOrInsertBlank() {
		String query = "";
		String doc = "";
		iDSSClient.upsert(query, doc);
	}

}
