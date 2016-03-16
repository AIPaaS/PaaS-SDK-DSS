package test.com.ai.paas.ipaas.dss.dssclient;

import org.junit.Before;
import org.junit.Test;

import test.com.ai.paas.ipaas.dss.dssclient.base.DSSClient;

import com.ai.paas.ipaas.dss.interfaces.IDSSClient;

public class SaveBytStr2Test extends DSSClient {
	private IDSSClient iDSSClient = null;

	@Before
	public void setUp() throws Exception {
		iDSSClient = super.getClient();
	}

	/*** 正常情况测试 */
	@Test
	public void save() {
		byte[] byte0 = "123456789".getBytes();
		String str1 = "thenormaltest";
		String key = "555b04fa3943e1e4d58a39e5";
		System.out.println(iDSSClient.save(byte0, str1));
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void saveFirstNull() {
		byte[] byte0 = null;
		String str1 = "thenormaltest";
		iDSSClient.save(byte0, str1);
	}

	/*** null测试 */
	@Test
	public void saveSecondNull() {
		byte[] byte0 = "12345".getBytes();
		String str1 = null;
		iDSSClient.save(byte0, str1);
	}

	/*** 空对象 */
	@Test
	public void saveFirstBlank() {
		byte[] byte0 = new byte[0];
		String str1 = "thenormaltest";
		iDSSClient.save(byte0, str1);
	}

	/*** 空对象 */
	@Test
	public void saveSecondBlank() {
		byte[] byte0 = "12345".getBytes();
		String str1 = "";
		iDSSClient.save(byte0, str1);
	}

}
