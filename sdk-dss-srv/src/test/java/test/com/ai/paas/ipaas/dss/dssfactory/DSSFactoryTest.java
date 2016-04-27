package test.com.ai.paas.ipaas.dss.dssfactory;

import org.junit.Test;

import com.ai.paas.ipaas.dss.DSSFactory;
import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;

public class DSSFactoryTest {
	private static AuthDescriptor ad = null;
	@SuppressWarnings("unused")
	private static IDSSClient dc = null;
	private static final String URL = "http://10.1.228.198:14821/iPaas-Auth/service/check";
	private static final String USER_NAME = "zhanglei11@asiainfo.com";
	private static final String PASSWORD = "1234567";
	private static final String SERVICE_ID = "DSS001";

	@Test
	public void setup() throws Exception {
		ad = new AuthDescriptor(URL, USER_NAME, PASSWORD, SERVICE_ID);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupUrlNull() throws Exception {
		ad = new AuthDescriptor(null, USER_NAME, PASSWORD, SERVICE_ID);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupUsernameNull() throws Exception {
		ad = new AuthDescriptor(URL, null, PASSWORD, SERVICE_ID);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupPwdNull() throws Exception {
		ad = new AuthDescriptor(URL, USER_NAME, null, SERVICE_ID);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupservIdNull() throws Exception {
		ad = new AuthDescriptor(URL, USER_NAME, PASSWORD, null);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupUrlUsernameNull() throws Exception {
		ad = new AuthDescriptor(null, null, PASSWORD, SERVICE_ID);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupUsernamePwdNull() throws Exception {
		ad = new AuthDescriptor(URL, null, null, SERVICE_ID);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupPwdServIdNull() throws Exception {
		ad = new AuthDescriptor(URL, USER_NAME, null, null);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupUrlServIdNull() throws Exception {
		ad = new AuthDescriptor(null, USER_NAME, PASSWORD, null);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupServIdNotNull() throws Exception {
		ad = new AuthDescriptor(null, null, null, SERVICE_ID);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupUsernameNotNull() throws Exception {
		ad = new AuthDescriptor(null, USER_NAME, null, null);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupPwdNotNull() throws Exception {
		ad = new AuthDescriptor(null, null, PASSWORD, null);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupUrlNotNull() throws Exception {
		ad = new AuthDescriptor(URL, null, null, null);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupAllNull() throws Exception {
		ad = new AuthDescriptor(null, null, null, null);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupUrlBlank() throws Exception {
		ad = new AuthDescriptor("", USER_NAME, PASSWORD, SERVICE_ID);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupUsernameBlank() throws Exception {
		ad = new AuthDescriptor(URL, "", PASSWORD, SERVICE_ID);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupPwdBlank() throws Exception {
		ad = new AuthDescriptor(URL, USER_NAME, "", SERVICE_ID);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupservIdBlank() throws Exception {
		ad = new AuthDescriptor(URL, USER_NAME, PASSWORD, "");
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupUrlUsernameBlank() throws Exception {
		ad = new AuthDescriptor("", "", PASSWORD, SERVICE_ID);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupUsernamePwdBlank() throws Exception {
		ad = new AuthDescriptor(URL, "", "", SERVICE_ID);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupPwdServIdBlank() throws Exception {
		ad = new AuthDescriptor(URL, USER_NAME, "", "");
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupUrlServIdBlank() throws Exception {
		ad = new AuthDescriptor("", USER_NAME, PASSWORD, "");
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupServIdNotBlank() throws Exception {
		ad = new AuthDescriptor("", "", "", SERVICE_ID);
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupUsernameNotBlank() throws Exception {
		ad = new AuthDescriptor("", USER_NAME, "", "");
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupPwdNotBlank() throws Exception {
		ad = new AuthDescriptor("", "", PASSWORD, "");
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupUrlNotBlank() throws Exception {
		ad = new AuthDescriptor(URL, "", "", "");
		dc = DSSFactory.getClient(ad);
	}

	@Test(expected = Exception.class)
	public void setupAllBlank() throws Exception {
		ad = new AuthDescriptor("", "", "", "");
		dc = DSSFactory.getClient(ad);
	}

}
