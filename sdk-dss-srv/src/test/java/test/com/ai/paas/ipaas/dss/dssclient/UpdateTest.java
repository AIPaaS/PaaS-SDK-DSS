package test.com.ai.paas.ipaas.dss.dssclient;

import org.junit.Before;
import org.junit.Test;

import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;

import test.com.ai.paas.ipaas.dss.dssclient.base.DSSClient;


public class UpdateTest extends DSSClient {
	private IDSSClient iDSSClient = null;

	@Before
	public void setUp() throws Exception {
		iDSSClient = super.getClient();
	}

	/*** 正常情况测试 */
	@Test
	public void update() {
		String str0 = "555af67539434c28d050916a";
		byte[] byte1 = "123456789".getBytes();
		iDSSClient.update(str0, byte1);
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void updateFirstNull() {
		String str0 = null;
		byte[] byte1 = "thenormaltest".getBytes();
		iDSSClient.update(str0, byte1);
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void updateSecondNull() {
		String str0 = "thenormaltest";
		byte[] byte1 = null;
		iDSSClient.update(str0, byte1);
	}

	/*** 空对象 */
	@Test(expected = Exception.class)
	public void updateFirstBlank() {
		String str0 = "";
		byte[] byte1 = "thenormaltest".getBytes();
		iDSSClient.update(str0, byte1);
	}

	/*** 空对象 */
	@Test
	public void updateSecondBlank() {
		String str0 = "555af67539434c28d050916a";
		byte[] byte1 = new byte[0];
		iDSSClient.update(str0, byte1);
	}

}
