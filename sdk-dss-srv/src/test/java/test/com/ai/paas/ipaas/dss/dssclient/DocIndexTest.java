package test.com.ai.paas.ipaas.dss.dssclient;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.com.ai.paas.ipaas.dss.dssclient.base.DSSClient;

import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;

public class DocIndexTest extends DSSClient {
	private IDSSClient iDSSClient = null;

	@Before
	public void setUp() throws Exception {
		iDSSClient = super.getClient();
		// 准备些数据
		iDSSClient.deleteAll();
		iDSSClient.insert("this is a test!");
		String doc = "{'age':23,'title':'test is test!'}";
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
	public void addIndex() {
		String field = "age";
		iDSSClient.addIndex(field, false);
		assertTrue(iDSSClient.isIndexExist(field));
	}

	/*** 正常情况测试 */
	@Test
	public void dropIndex() {
		String field = "age";
		iDSSClient.dropIndex(field);
		assertFalse(iDSSClient.isIndexExist(field));
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void addIndexNull() {
		String field = null;
		iDSSClient.addIndex(field, false);
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void dropIndexNull() {
		String field = null;
		assertFalse(iDSSClient.isIndexExist(field));
	}

	/*** 空测试 */
	@Test(expected = Exception.class)
	public void addIndexBlank() {
		String field = "";
		iDSSClient.addIndex(field, false);
	}

	/*** 空测试 */
	@Test(expected = Exception.class)
	public void dropIndexBlank() {
		String field = "";
		assertFalse(iDSSClient.isIndexExist(field));
	}

}
