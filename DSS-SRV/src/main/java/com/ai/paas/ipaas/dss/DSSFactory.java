package com.ai.paas.ipaas.dss;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ai.paas.ipaas.ccs.inner.CCSComponentFactory;
import com.ai.paas.ipaas.dss.base.interfaces.IDSSClient;
import com.ai.paas.ipaas.dss.impl.DSSSrvClient;
import com.ai.paas.ipaas.uac.service.UserClientFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.ai.paas.ipaas.uac.vo.AuthResult;
import com.ai.paas.ipaas.util.Assert;
import com.ai.paas.ipaas.util.CiperUtil;
import com.google.gson.Gson;

public class DSSFactory {

	private final static String DSS_CONFIG_COMMON_PATH = "/DSS/COMMON";
	private final static String DSS_CONFIG_PATH = "/DSS/";
	private final static String MONGO_USER = "username";
	private final static String MONGO_PASSWORD = "password";
	private final static String MONGO_HOST = "hosts";
	private final static String MONGO_REDIS_HOST = "redisHosts";

	private final static String PWD_KEY = "BaryTukyTukyBary";

	private static Map<String, IDSSClient> DSSClients = new ConcurrentHashMap<>();
	private static final Logger log = LogManager.getLogger(DSSFactory.class);

	private DSSFactory() {
		// 禁止私有化
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static IDSSClient getClient(AuthDescriptor ad) throws Exception {
		IDSSClient DSSClient = null;
		log.info("Check Formal Parameter AuthDescriptor ...");
		Assert.notNull(ad, "com.ai.paas.ipaas.common.auth_info_null");
		Assert.notNull(ad.getServiceId(), "com.ai.paas.ipaas.common.srvid_null");
		String srvId = ad.getServiceId();
		// 服务号要检验
		// 传入用户描述对象，用户认证地址，服务申请号
		// 进行用户认证
		log.info("Check AuthResult ...");
		// 认证通过后，判断是否存在已有实例，有，直接返回
		// 单例标签
		String instanceKey = ad.getPid() + "_" + ad.getServiceId();
		if (DSSClients.containsKey(instanceKey)) {
			DSSClient = DSSClients.get(instanceKey);
			return DSSClient;
		}
		AuthResult authResult = UserClientFactory.getUserClient().auth(ad);
		// 开始初始化
		Assert.notNull(authResult.getConfigAddr(),
				"com.ai.paas.ipaas.common.zk_addr_null");
		Assert.notNull(authResult.getConfigUser(),
				"com.ai.paas.ipaas.common.zk_user_null");
		Assert.notNull(authResult.getConfigPasswd(),
				"com.ai.paas.ipaas.common.zk_passwd_null");
		Assert.notNull(authResult.getUserId(),
				"com.ai.paas.ipaas.common.user_id_null");
		// 获取内部zk地址后取得该用户的cache配置信息，返回JSON String
		// 获取该用户申请的cache服务配置信息
		log.info("Get DSSConf ...");
		String DSSConf = CCSComponentFactory.getConfigClient(
				authResult.getConfigAddr(), authResult.getConfigUser(),
				authResult.getConfigPasswd()).get(DSS_CONFIG_COMMON_PATH);
		String DSSRedisConf = CCSComponentFactory.getConfigClient(
				authResult.getConfigAddr(), authResult.getConfigUser(),
				authResult.getConfigPasswd()).get(DSS_CONFIG_PATH + srvId);
		Gson gson = new Gson();
		Map DSSConfMap = gson.fromJson(DSSConf, Map.class);
		String hosts = (String) DSSConfMap.get(MONGO_HOST);
		String username = (String) DSSConfMap.get(MONGO_USER);
		String password = CiperUtil.decrypt(PWD_KEY,
				(String) DSSConfMap.get(MONGO_PASSWORD));
		String redisHosts = (String) DSSConfMap.get(MONGO_REDIS_HOST);
		// String DSSRWConf =
		// confBase.getConfigFromWritable(DSS_CONFIG_PATH+"/"+srvId);
		// String DSSRWConf =
		// CCSComponentFactory.getConfigClient(authResult.getConfigAddr(),
		// authResult.getConfigUser(),authResult.getConfigPasswd()).get
		// 封装成配置对象
		Map DSSRedisConfMap = gson.fromJson(DSSRedisConf, Map.class);
		String userId = authResult.getUserId();
		System.out.println();
		DSSClient = new DSSSrvClient(hosts, userId, username, password,
				redisHosts, DSSRedisConfMap);
		DSSClients.put(instanceKey, DSSClient);
		return DSSClient;
	}

}
