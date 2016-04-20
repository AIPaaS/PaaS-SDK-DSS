package test.com.ai.paas.ipaas.dss.dssclient;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import test.com.ai.paas.ipaas.dss.dssclient.base.DSSClient;

import com.ai.paas.ipaas.dss.interfaces.IDSSClient;

public class UpdateTestFile extends DSSClient {
	private IDSSClient iDSSClient = null;

	@Before
	public void setUp() throws Exception {
		iDSSClient = super.getClient();
	}

	/*** 正常情况测试 */
	@Test
	public void update() {
		String str0 = "556c19201fda9a2430b3121a";
		File file = new File("C://Users//CYM//Desktop//cat.jpg");
		iDSSClient.update(str0, file);
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void updateFirstNull() {
		String str0 = null;
		File file = new File("C://Users//CYM//Desktop//cat.jpg");
		iDSSClient.update(str0, file);
	}

	/*** null测试 */
	@Test(expected = Exception.class)
	public void updateSecondNull() {
		String str0 = "thenormaltest";
		File file = null;
		iDSSClient.update(str0, file);
	}

	/*** 空对象 */
	@Test(expected = Exception.class)
	public void updateFirstBlank() {
		String str0 = "";
		File file = new File("C://Users//CYM//Desktop//cat.jpg");
		iDSSClient.update(str0, file);
	}

	/*** 空对象 */
	@Test
	public void updateSecondBlank() {
		String str0 = "556c19201fda9a2430b3121a";
		File file = new File("");
		iDSSClient.update(str0, file);
	}

}
