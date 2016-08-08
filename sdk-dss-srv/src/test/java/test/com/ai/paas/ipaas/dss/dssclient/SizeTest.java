package test.com.ai.paas.ipaas.dss.dssclient;

import java.io.FileNotFoundException;

import org.junit.Before;
import org.junit.Test;

import test.com.ai.paas.ipaas.dss.dssclient.base.DSSClient;

import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;


public class SizeTest extends DSSClient {
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
	public void getSize() {
		iDSSClient.getSize();
	}

}
