package com.ai.paas.ipaas.dss.base.impl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.mongodb.ServerAddress;

//import com.ai.paas.ipaas.config.inner.IConfigClient;

public class DSSHelper {

	public static List<ServerAddress> Str2SAList(String hosts) {
		String[] hostsArray = hosts.split(";");
		List<ServerAddress> saList = new ArrayList<>();
		String[] address = null;
		for (String host : hostsArray) {
			address = host.split(":");
			try {
				saList.add(new ServerAddress(address[0], Integer
						.parseInt(address[1])));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return saList;
	}

	public static String getFileType(String fileName) {
		String[] fileInfo = fileName.split("\\.");
		return fileInfo[1];
	}

	public static long getFileSize(File file) {
		return Long.parseLong(file.length() + "");
	}

	public static long getFileSize(byte[] file) {
		return Long.parseLong(file.length + "");
	}

	public static long okSize(long a, long b) {
		return Long.parseLong((a - b) + "");
	}

	// public static void setDSSRWConf(IConfigClient confBase,String
	// dbName,Map<String,String> DSSRWConfMap) {
	// Gson gson = new Gson();
	// confBase.modifyConfig(DSS_CONFIG_PATH+dbName, gson.toJson(DSSRWConfMap));
	// }

	public static byte[] toByteArray(InputStream input) throws Exception {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		byte[] buffer = new byte[4096];
		int n = 0;
		while (-1 != (n = input.read(buffer))) {
			output.write(buffer, 0, n);
		}
		return output.toByteArray();
	}

	public static double byte2M(Long size) {
		BigDecimal bd = new BigDecimal(size);
		return bd.divide(new BigDecimal(1024 * 1024), 3,
				BigDecimal.ROUND_HALF_UP).doubleValue();
	}

	public static long M2byte(double size) {
		BigDecimal sizeBD = new BigDecimal(size);
		double result = sizeBD.multiply(new BigDecimal(1024 * 1024))
				.doubleValue();
		return Math.round(result);
	}


}
