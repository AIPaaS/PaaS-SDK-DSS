package com.ai.paas.ipaas.dss.base.interfaces;

import java.io.File;
import java.util.Date;

public interface IDSSClient {
	/**
	 * 存储文件
	 * 
	 * @param file
	 *            待存入文件
	 * @param remark
	 *            文件描述
	 * @return 文件存储的位置对应的id，用于取出文件
	 */
	public String save(File file, String remark);

	/**
	 * 存储文件
	 * 
	 * @param file
	 *            待存入文件内容
	 * @param remark
	 *            文件描述
	 * @return 文件存储的位置对应的id，用于取出文件
	 */
	public String save(byte[] bytes, String remark);

	/**
	 * 读取指定文件
	 * 
	 * @param id
	 *            待读取文件的id
	 * @return 文件内容
	 */
	public byte[] read(String id);

	/**
	 * 删除指定文件
	 * 
	 * @param id
	 *            待删除文件的id
	 * @return true成功 false失败
	 */
	public boolean delete(String id);

	/**
	 * 修改文件
	 * 
	 * @param id
	 *            待修改文件的id
	 * @param bytes
	 *            新文件内容
	 */
	public void update(String id, byte[] bytes);

	/**
	 * 修改文件
	 * 
	 * @param id
	 *            待修改文件的id
	 * @param file
	 */
	public void update(String id, File file);

	/**
	 * 获取最近一次操作文件时间
	 * 
	 * @param id
	 *            待查询文件的id
	 * @return 最近一次操作文件时间
	 */
	public Date getLastUpdateTime(String id);

}
