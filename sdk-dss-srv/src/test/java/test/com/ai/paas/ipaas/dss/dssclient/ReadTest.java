package test.com.ai.paas.ipaas.dss.dssclient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import org.junit.Before;
import org.junit.Test;

import com.ai.paas.ipaas.dss.IDSSClient;

import test.com.ai.paas.ipaas.dss.dssclient.base.DSSClient;


public class ReadTest extends DSSClient {
	private IDSSClient iDSSClient = null;

	@Before
	public void setUp() throws Exception {
		iDSSClient = super.getClient();
	}

	/***
	 * 正常情况测试
	 * 
	 * @throws FileNotFoundException
	 */
	@Test
	public void read() {
		try {
			byte[] fileByte = iDSSClient.read("557673c21fda9a35b8339a7f");
			// byte[] fileByte = iDSSClient.read("556bb9cb1fda9a2628fa656a");
			File file = new File("C://Users//CYM//Desktop//test//黑猫.txt");
			FileOutputStream fos = new FileOutputStream(file);
			fos.write(fileByte);
			fos.flush();
			fos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*** null测试 */
	// TODO
	@Test(expected = Exception.class)
	public void readFirstNull() {
		String str0 = null;
		iDSSClient.read(str0);
	}

	/*** 空对象 */
	@Test(expected = Exception.class)
	public void readFirstBlank() {
		String str0 = "";
		iDSSClient.read(str0);
	}

}
