package test.com.ai.paas.ipaas.dss.dssclient;

import org.junit.Before;
import org.junit.Test;

import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;

import test.com.ai.paas.ipaas.dss.dssclient.base.DSSClient;


public class DeleteTest extends DSSClient {
	private IDSSClient iDSSClient = null;

	@Before
	public void setUp() throws Exception {
		iDSSClient = super.getClient();
	}

	/*** 正常情况测试 */
	@Test
	public void delete() {
		String str0 = "555af67539434c28d050916a";
		iDSSClient.delete(str0);
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void deleteFirstNull() {
		String str0 = null;
		iDSSClient.delete(str0);
	}

	/*** 空对象 */
	@Test(expected = Exception.class)
	public void deleteFirstBlank() {
		String str0 = "";
		iDSSClient.delete(str0);
	}

}
