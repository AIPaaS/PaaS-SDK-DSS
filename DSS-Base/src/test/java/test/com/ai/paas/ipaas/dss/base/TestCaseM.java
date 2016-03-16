package test.com.ai.paas.ipaas.dss.base;


import java.io.File;
import java.io.FileOutputStream;

import org.junit.Before;
import org.junit.Test;

import com.ai.paas.ipaas.dss.base.DSSFactory;
import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;

public class TestCaseM {

	private IDSSClient iDSSClient = null;

	@Before
	public void setUp() throws Exception {
		iDSSClient =DSSFactory.getClient("{\"mongoServer\":\"10.1.228.200:37017;10.1.228.202:37017\",\"database\":\"admin\",\"userName\":\"sa\",\"password\":\"sa\"}");
	}
	
	@Test
	public void save() {
		byte[] byte0 = "123456789".getBytes();
		String str1 = "thenormaltest";
		System.out.println(iDSSClient.save(byte0, str1));
	}
	
	@Test
	public void read() {
		try {
			byte[] fileByte = iDSSClient.read("56e91e76fb3b87629f1e4f00");
			File file = new File("/Users/liwenxian/Downloads/1/liwx.txt");
			FileOutputStream fos = new FileOutputStream(file);
			fos.write(fileByte);
			fos.flush();
			fos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testAll() {
		try {
			
			for(int i=0;i<20;i++){
				byte[] byte0 =( i+"123456789").getBytes();
				String str1 = "thenormaltest";
				String id = iDSSClient.save(byte0, str1);
				System.out.println(id);
				
				byte[] fileByte = iDSSClient.read(id);
				File file = new File("/Users/liwenxian/Downloads/1/liwx"+i+".txt");
				FileOutputStream fos = new FileOutputStream(file);
				fos.write(fileByte);
				fos.flush();
				fos.close();
			}
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
