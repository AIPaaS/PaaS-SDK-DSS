package test.com.ai.paas.ipaas.dss.dssclient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;

import test.com.ai.paas.ipaas.dss.dssclient.base.DSSClient;


public class DocInsertTest extends DSSClient {
	private IDSSClient iDSSClient = null;

	@Before
	public void setUp() throws Exception {
		iDSSClient = super.getClient();
	}
	@After
	public void destroy()throws Exception {
		iDSSClient.deleteAll();
	}
	/*** 正常情况测试 */
	@Test
	public void insertString() {
		String content = "555af67539434c28d050916a";
		iDSSClient.insert(content);
	}

	/*** 正常情况测试 */
	@Test
	public void insertJson() {
		String doc = "{'age':23,'title':'test is test!'}";
		iDSSClient.insertJSON(doc);
	}
	/*** 正常情况测试 */
	@Test
	public void insertMap() {
		Map<String,String> doc=new HashMap<>();
		doc.put("age", "24");
		doc.put("author", "xiaoming");
		iDSSClient.insert(doc);
	}
	
	/*** 正常情况测试 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void insertBatch() {
		Map<String,String> doc=new HashMap<>();
		doc.put("age", "24");
		doc.put("author", "xiaoming");
		List docs=new ArrayList<>();
		docs.add(doc);
		doc=new HashMap<>();
		doc.put("age", "25");
		doc.put("author", "xiaoming123");
		docs.add(doc);
		iDSSClient.insertBatch(docs);
	}
	
	/*** null测试 */
	@Test(expected = Exception.class)
	public void insertStringNull() {
		String content = null;
		iDSSClient.insert(content);
	}
	/*** null测试 */
	@Test(expected = Exception.class)
	public void insertJSonNull() {
		String doc = null;
		iDSSClient.insertJSON(doc);
	}
	/*** null测试 */
	@Test(expected = Exception.class)
	public void insertMapNull() {
		Map<String,String> doc=null;
		iDSSClient.insert(doc);
	}
	
	/*** null测试 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test(expected = Exception.class)
	public void insertBacthNull() {
		List docs=null;
		iDSSClient.insertBatch(docs);
	}

	/*** 空测试 */
	@Test(expected = Exception.class)
	public void insertStringBlank() {
		String content = "";
		iDSSClient.insert(content);
	}
	/*** 空测试 */
	@Test(expected = Exception.class)
	public void insertJSonBlank() {
		String doc = "";
		iDSSClient.insertJSON(doc);
	}
	/*** 空测试 */
	@Test(expected = Exception.class)
	public void insertMapBlank() {
		Map<String,String> doc=new HashMap<>();
		iDSSClient.insert(doc);
	}

	/*** 空测试 */
	@SuppressWarnings("unchecked")
	@Test(expected = Exception.class)
	public void insertBatchBlank() {
		@SuppressWarnings("rawtypes")
		List docs=new ArrayList<>();
		iDSSClient.insertBatch(docs);
	}
}
