package com.ai.paas.ipaas.dss.impl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ServerAddress;

public class DSSHelper {
    private static final Logger log = LoggerFactory.getLogger(DSSHelper.class);

    public static List<ServerAddress> toAddressList(String hosts) {
        String[] hostsArray = hosts.split(";|,");
        List<ServerAddress> saList = new ArrayList<>();
        String[] address = null;
        for (String host : hostsArray) {
            address = host.split(":");
            try {
                saList.add(new ServerAddress(address[0], Integer.parseInt(address[1])));
            } catch (Exception e) {
                log.error("", e);
            }
        }
        return saList;
    }

    public static String getFileType(String fileName) {
        String[] fileInfo = fileName.split("\\.");
        return fileInfo[1];
    }

    public static long getFileSize(File file) {
        if (null == file)
            return 0;
        return Long.parseLong(file.length() + "");
    }

    public static long getFileSize(byte[] file) {
        if (null == file)
            return 0;
        return Long.parseLong(file.length + "");
    }

    @SuppressWarnings("rawtypes")
    public static long getListSize(List list) {
        if (null == list)
            return 0;
        long total = 0;
        for (int i = 0; i < list.size(); i++) {
            total += list.get(i).toString().getBytes(StandardCharsets.UTF_8).length;
        }
        return total;
    }

    @SuppressWarnings("rawtypes")
    public static long getSize(Object obj) {
        if (null == obj)
            return 0;
        if (obj instanceof File)
            return getFileSize((File) obj);
        else if (obj instanceof byte[])
            return getFileSize((byte[]) obj);
        else if (obj instanceof List)
            return getListSize((List) obj);
        else
            return obj.toString().getBytes(StandardCharsets.UTF_8).length;
    }

    public static long okSize(long a, long b) {
        return Long.parseLong((a - b) + "");
    }

    public static byte[] toByteArray(InputStream input) throws IOException {
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
        return bd.divide(new BigDecimal(1024 * 1024), 3, RoundingMode.HALF_UP).doubleValue();
    }

    public static long tobyte(double size) {
        BigDecimal sizeBD = BigDecimal.valueOf(size);
        double result = sizeBD.multiply(new BigDecimal(1024 * 1024)).doubleValue();
        return Math.round(result);
    }

}
