package test.com.ai.paas.ipaas.dss.dssclient;

import org.junit.Test;

import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;

import test.com.ai.paas.ipaas.dss.dssclient.base.DSSClient;


public class SaveFileStr2Test extends DSSClient {
	private static IDSSClient iDSSClient = null;
	private static java.io.File file0 = new java.io.File("E://软件//桌面//bin.xml");
	static {
		try {
			iDSSClient = DSSClient.getClient();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// @Before
	// public void setUp() throws Exception {
	// iDSSClient = super.getClient();
	// }

	/*** 正常情况测试 */
	@Test
	public void save() {
		System.out.println(iDSSClient.save(file0, ""));

	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void saveFirstNull() {
		java.io.File file0 = null;
		String str1 = "thenormaltest";
		iDSSClient.save(file0, str1);
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void saveSecondNull() {
		// TODO;
		java.io.File file0 = new java.io.File("");
		String str1 = null;
		iDSSClient.save(file0, str1);
	}

	/*** 空对象 */
	@Test(expected = Exception.class)
	public void saveFirstBlank() {
		// TODO;
		java.io.File file0 = new java.io.File("");
		String str1 = "thenormaltest";
		iDSSClient.save(file0, str1);
	}

	/*** 空对象 */
	@Test(expected = Exception.class)
	public void saveSecondBlank() {
		// TODO;
		java.io.File file0 = new java.io.File("");
		String str1 = "";
		iDSSClient.save(file0, str1);
	}

}
