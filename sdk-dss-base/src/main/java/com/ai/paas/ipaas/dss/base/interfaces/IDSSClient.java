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
	 * 根据文件名称读取文件，文件名要唯一
	 * 
	 * @param fileName
	 * @return
	 */
	public byte[] readByName(String fileName);

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
	 * @return 新的id
	 */
	public String update(String id, byte[] bytes);

	/**
	 * 修改文件
	 * 
	 * @param id
	 *            待修改文件的id
	 * @param file
	 * @return 新的id
	 */
	public String update(String id, File file);

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
	 * 
	 * @param content
	 *            普通字符串，将以{"content":content}插入
	 * @return 唯一标识，主键
	 */
	public String insert(String content);

	/**
	 * 插入json格式的文档
	 * 
	 * @param doc
	 *            json格式文档{"content":"xxxx","title":"xxxx"}
	 * @return 唯一标识，主键
	 */
	public String insertJSON(String doc);

	/**
	 * 插入文档
	 * 
	 * @param doc
	 *            字段：字段值
	 * @return 唯一标识，主键
	 */
	@SuppressWarnings("rawtypes")
	public String insert(Map doc);

	/**
	 * 插入多个文档
	 * 
	 * @param docs
	 *            字段：字段值对象列表
	 */
	public void insertBatch(List<Map<String, Object>> docs);

	/**
	 * 按主键删除
	 * 
	 * @param id
	 * @return 受影响条数
	 */
	public long deleteById(String id);

	/**
	 * 可删除多个或者单个文档
	 * 
	 * @param doc
	 *            {"content":"xxxx","title":"xxxx"}或{"_id":"xxxx"}
	 * @return 受影响条数
	 */
	public long deleteByJson(String doc);

	/**
	 * 可删除多个或者单个文档
	 * 
	 * @param doc
	 *            {"content":"xxxx","title":"xxxx"}或{"_id":"xxxx"}
	 * @return 受影响条数
	 */
	@SuppressWarnings("rawtypes")
	public long deleteByMap(Map doc);

	/**
	 * 判断集合是否存在
	 * 
	 * @param collectionName
	 * @return
	 */
	public boolean collectionExists(final String collectionName);

	/**
	 * 慎重使用，会删除所有的数据
	 * 
	 * @return 受影响条数
	 */
	public long deleteAll();

	/**
	 * 可删除多个或者单个文档
	 * 
	 * @param docs
	 *            {"content":"xxxx","title":"xxxx"}或{"_id":"xxxx"}
	 * @return 受影响条数
	 */
	public long deleteBatch(List<Map<String, Object>> docs);

	/**
	 * 根据主键更新
	 * 
	 * @param id
	 *            主键
	 * @param doc
	 *            {"content":"xxxx","title":"xxxx"}
	 * @return 受影响条数
	 */
	public long updateById(String id, String doc);

	/**
	 * 根据条件更新
	 * 
	 * @param query
	 *            {"content":"xxxx","title":"xxxx"}
	 * @param doc
	 *            {"author":"xxxx","date":"xxxx"}
	 * @return 受影响条数
	 */
	public long update(String query, String doc);

	/**
	 * 如果存在则更新，不存在插入 doc
	 * 
	 * @param query
	 *            {"content":"xxxx","title":"xxxx"}
	 * @param doc
	 *            {"author":"xxxx","date":"xxxx"}
	 * @return 受影响条数
	 */
	public long upsert(String query, String doc);

	/**
	 * 根据主键查找文档
	 * 
	 * @param id
	 * @return {"author":"xxxx","date":"xxxx"}
	 */
	public String findById(String id);

	/**
	 * 根据查询条件返回多条文档
	 * 
	 * @param doc
	 *            {"author":"xxxx","date":"xxxx"}
	 * @return json数组
	 */
	@SuppressWarnings("rawtypes")
	public String find(Map doc);

	/**
	 * 根据查询条件返回多条文档,限制返回1000条，可以使用分页进行查询
	 * 
	 * @param query
	 *            {"author":"xxxx","date":"xxxx"}或者{"startDate": {$lt: endDate}}
	 * @return json数组
	 */
	public String find(String query);

	/**
	 * 分页查询
	 * 
	 * @param query
	 *            {"author":"xxxx","date":"xxxx"}
	 * @param pageNumber
	 * @param pageSize
	 * @return json数组
	 */
	public String query(String query, int pageNumber, int pageSize);

	/**
	 * @deprecated 查询总条数，请使用 count方法
	 * @param query
	 *            {"author":"xxxx","date":"xxxx"}
	 * @return
	 */
	public long getCount(String query);

	/**
	 * 查询总条数，请使用 count方法
	 * 
	 * @param query
	 *            {"author":"xxxx","date":"xxxx"}
	 * @return
	 */
	public long count(String query);

	/**
	 * 增加索引,建议不要创建太多索引
	 * 
	 * @param field
	 *            要建立索引的字段名，索引名字idx_field
	 * @param unique
	 *            是否数据唯一，true不允许插入重复数据
	 */
	public void addIndex(String field, boolean unique);

	/**
	 * 删除索引
	 * 
	 * @param field
	 */
	public void dropIndex(String field);

	/**
	 * 删除所有索引
	 */
	public void dropAllIndex();

	/**
	 * 判断索引是否存在
	 * 
	 * @param field
	 * @return
	 */
	public boolean isIndexExist(String field);

	/**
	 * 获取当前的存储大小
	 * 
	 * @return 返回的大小为字节数，且只是数据和文件大小的和
	 */
	public Long getSize();

	/**
	 * 判断一个点是否在多边形区域内
	 * 
	 * 
	 * @param field
	 *            标识坐标的字段
	 * @param mongoId
	 *            具有的多边形坐标的行主键标识 5ab1ffb696ca0318249ef4d8
	 * @param point
	 *            以[lng经度-180到180,lat纬度-90到90]，维度以正负号表示，经度一样
	 * @return
	 */
	public boolean withinPolygon(final String field, final String mongoId, final double[] point);

	/**
	 * 判断一个点是否在多边形区域内
	 * 
	 * @param field
	 *            标识坐标的字段 如geom
	 * 
	 * @param coordinates
	 *            坐标数组，经度，维度对，一定要是闭合的，即第一个坐标和最后一个坐标一样，[ 117.137666, 39.209547 ],
	 *            [ 117.131629, 39.207087 ], [ 117.132204, 39.201719 ], [
	 *            117.135797, 39.196798 ], [ 117.14169, 39.201048 ], [
	 *            117.145571, 39.195009 ], [ 117.136228, 39.182034 ], [
	 *            117.132635, 39.162454 ], [ 117.135366, 39.157978 ], [
	 *            117.13321, 39.153502 ], [ 117.12703, 39.152047 ], [
	 *            117.128898, 39.149361 ], [ 117.116825, 39.139623 ], [
	 *            117.138672, 39.12048 ], [ 117.143271, 39.096963 ], [
	 *            117.129329, 39.090131 ], [ 117.146433, 39.080609 ], [
	 *            117.154051, 39.084754 ], [ 117.174029, 39.066604 ], [
	 *            117.195732, 39.072878 ], [ 117.19142, 39.075791 ], [
	 *            117.186965, 39.094499 ], [ 117.186533, 39.12507 ], [
	 *            117.188402, 39.142869 ], [ 117.201194, 39.140519 ], [
	 *            117.201769, 39.152606 ], [ 117.183084, 39.168944 ], [
	 *            117.187252, 39.173979 ], [ 117.185096, 39.187515 ], [
	 *            117.178772, 39.187403 ], [ 117.166411, 39.196016 ], [
	 *            117.154194, 39.194562 ], [ 117.150314, 39.197693 ], [
	 *            117.145427, 39.195009 ], [ 117.14169, 39.20116 ], [ 117.14672,
	 *            39.204068 ], [ 117.137809, 39.209659 ], [ 117.137666,
	 *            39.209547 ]
	 * @param point
	 *            以[lng经度,lat维度]，维度以正负号表示，经度一样
	 * @return
	 */
	public boolean withinPolygon(final String field, final List<double[]> coordinates, final double[] point);

	/**
	 * 查找指定多边形内的坐标点
	 * 
	 * @param field
	 *            代表坐标的字段
	 * @param coordinates
	 *            坐标数组，经度，维度对，一定要是闭合的，即第一个坐标和最后一个坐标一样，[ 117.137666, 39.209547 ],
	 *            [ 117.131629, 39.207087 ], [ 117.132204, 39.201719 ], [
	 *            117.135797, 39.196798 ], [ 117.14169, 39.201048 ], [
	 *            117.145571, 39.195009 ], [ 117.136228, 39.182034 ], [
	 *            117.132635, 39.162454 ], [ 117.135366, 39.157978 ], [
	 *            117.13321, 39.153502 ], [ 117.12703, 39.152047 ], [
	 *            117.128898, 39.149361 ], [ 117.116825, 39.139623 ], [
	 *            117.138672, 39.12048 ], [ 117.143271, 39.096963 ], [
	 *            117.129329, 39.090131 ], [ 117.146433, 39.080609 ], [
	 *            117.154051, 39.084754 ], [ 117.174029, 39.066604 ], [
	 *            117.195732, 39.072878 ], [ 117.19142, 39.075791 ], [
	 *            117.186965, 39.094499 ], [ 117.186533, 39.12507 ], [
	 *            117.188402, 39.142869 ], [ 117.201194, 39.140519 ], [
	 *            117.201769, 39.152606 ], [ 117.183084, 39.168944 ], [
	 *            117.187252, 39.173979 ], [ 117.185096, 39.187515 ], [
	 *            117.178772, 39.187403 ], [ 117.166411, 39.196016 ], [
	 *            117.154194, 39.194562 ], [ 117.150314, 39.197693 ], [
	 *            117.145427, 39.195009 ], [ 117.14169, 39.20116 ], [ 117.14672,
	 *            39.204068 ], [ 117.137809, 39.209659 ], [ 117.137666,
	 *            39.209547 ]
	 * @return 返回json数据，里面为找到的 points。
	 */
	public String findGeoWithinPolygon(final String field, List<double[]> coordinates);

	/**
	 * 对指定的地理坐标字段创建地理索引
	 * 
	 * @param field
	 */
	public void createGeoIndex(final String field);

	/**
	 * 关闭客户端
	 */
	public void close();

}
