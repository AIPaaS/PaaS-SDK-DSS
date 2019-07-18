package test.com.ai.paas.ipaas.dss.dssclient.base;

import com.ai.paas.ipaas.dss.DSSFactory;
import com.ai.paas.ipaas.dss.IDSSClient;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;

public class DSSClient {

	// private static final String URL =
	// "http://10.1.228.201:14821/iPaas-Auth/service/check";
	// private static final String USER_NAME = "chenym6@asiainfo.com";
	// private static final String PASSWORD = "1234567";
	// private static final String SERVICE_ID = "DSS001";
	
	private static final String URL = "http://10.1.245.4:19811/service-portal-uac-web/service/auth";
	private static final String PID = "B1F464FC22E745D79EE67B2691112795";
	private static final String PASSWORD = "123456";
	private static final String SERVICE_ID = "DSS001";
	private static AuthDescriptor ad = null;

	public static IDSSClient getClient() throws Exception {
		ad = new AuthDescriptor(URL, PID, PASSWORD, SERVICE_ID);
		return DSSFactory.getClient(ad);
	}
}
