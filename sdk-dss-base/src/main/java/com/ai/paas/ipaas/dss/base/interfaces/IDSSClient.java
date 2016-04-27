package com.ai.paas.ipaas.dss.base.interfaces;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Map;

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

	/**
	 * 获取文件的大小，如果不存在，则等于0
	 * 
	 * @param id
	 * @return
	 */
	public long getFileSize(String id);

	/**
	 * 判断文件是否存在
	 * 
	 * @param id
	 * @return
	 */
	public boolean isFileExist(String id);
	
	/**
	 * 插入普通字符串文档，collection默认为db名字,字符串名字为content
	 * @param content 普通字符串，将以{"content":content}插入
	 * @return 唯一标识，主键
	 */
	public abstract String insert(String content);

	/**
	 * 插入json格式的文档
	 * @param doc json格式文档{"content":"xxxx","title":"xxxx"}
	 * @return 唯一标识，主键
	 */
	public abstract String insertJSON(String doc);

	/**
	 * 插入文档
	 * @param doc 字段：字段值
	 * @return 唯一标识，主键
	 */
	@SuppressWarnings("rawtypes")
	public abstract String insert(Map doc);

	/**
	 * 插入多个文档
	 * @param docs 字段：字段值对象列表
	 */
	public abstract void insertBatch(List<Map<String, Object>> docs);

	/**
	 * 按主键删除
	 * @param id  
	 * @return 受影响条数
	 */
	public abstract int deleteById(String id);

	/**
	 * 可删除多个或者单个文档
	 * @param doc {"content":"xxxx","title":"xxxx"}或{"_id":"xxxx"}
	 * @return 受影响条数
	 */
	public abstract int deleteByJson(String doc);

	/**
	 * 可删除多个或者单个文档
	 * @param doc {"content":"xxxx","title":"xxxx"}或{"_id":"xxxx"}
	 * @return 受影响条数
	 */
	@SuppressWarnings("rawtypes")
	public abstract int deleteByMap(Map doc);

	/**
	 * 慎重使用，会删除所有的数据
	 * @return 受影响条数
	 */
	public abstract int deleteAll();

	/** 
	 * 可删除多个或者单个文档
	 * @param docs {"content":"xxxx","title":"xxxx"}或{"_id":"xxxx"}
	 * @return 受影响条数
	 */
	public abstract int deleteBatch(List<Map<String, Object>> docs);

	/**
	 * 根据主键更新
	 * @param id 主键
	 * @param doc {"content":"xxxx","title":"xxxx"}
	 * @return 受影响条数
	 */
	public abstract int updateById(String id, String doc);

	/**
	 * 根据条件更新
	 * @param query {"content":"xxxx","title":"xxxx"}
	 * @param doc	{"author":"xxxx","date":"xxxx"}
	 * @return 受影响条数
	 */
	public abstract int update(String query, String doc);

	/**
	 * 	如果存在则更新，不存在插入 doc
	 * @param query {"content":"xxxx","title":"xxxx"}
	 * @param doc   {"author":"xxxx","date":"xxxx"}
	 * @return 受影响条数
	 */
	public abstract int updateOrInsert(String query, String doc);

	/**
	 * 根据主键查找文档
	 * @param id 
	 * @return {"author":"xxxx","date":"xxxx"}
	 */
	public abstract String findById(String id);

	/**
	 * 根据查询条件返回多条文档
	 * @param doc {"author":"xxxx","date":"xxxx"}
	 * @return json数组
	 */
	@SuppressWarnings("rawtypes")
	public abstract String findOne(Map doc);

	/**
	 *  根据查询条件返回多条文档
	 * @param query {"author":"xxxx","date":"xxxx"}
	 * @return json数组
	 */
	public abstract String find(String query);

	/**
	 * 分页查询
	 * @param query {"author":"xxxx","date":"xxxx"}
	 * @param pageNumber 
	 * @param pageSize
	 * @return json数组
	 */
	public abstract String query(String query, int pageNumber, int pageSize);

	/**
	 * 查询总条数
	 * @param query {"author":"xxxx","date":"xxxx"}
	 * @return
	 */
	public abstract long getCount(String query);

	/**
	 * 增加索引,建议不要创建太多索引
	 * @param field 要建立索引的字段名，索引名字idx_field
	 * @param unique 是否数据唯一，true不允许插入重复数据
	 */
	public abstract void addIndex(String field, boolean unique);

	/**
	 * 删除索引
	 * @param field
	 */
	public abstract void dropIndex(String field);
	
	/**
	 * 判断索引是否存在
	 * @param field
	 * @return
	 */
	public abstract boolean isIndexExist(String field);

}
