package com.whaty.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.whaty.util.SpringUtil;
import com.whaty.platform.entity.exception.EntityException;
import com.whaty.platform.entity.service.GeneralService;

/*
 * Cassandra 解析工具类
 * 
/
public class parsingCassandraDataUtil {

	public final static String HOST = "192.168.*.*";
	private static final String KEYSPACE = "demo";
	public static final String COLUMNFAMILY = "pt";

	private static Cassandra.Client client = null;

	public static void connectCassandra() throws TException,
			InvalidRequestException, UnavailableException,
			UnsupportedEncodingException, NotFoundException, TimedOutException {

		// 包装好的socket
		TTransport tr = new TFramedTransport(new TSocket(HOST, 9160));
		TProtocol proto = new TBinaryProtocol(tr);
		client = new Cassandra.Client(proto);
		tr.open();
		if (!tr.isOpen()) {
			System.out.println("Failed to connect server!");
			return;
		} else {
			System.out.println("Success to connect server!");
		}
	}

	public static void process() throws TException, InvalidRequestException,
			UnavailableException, NotFoundException, TimedOutException,
			IOException {
		connectCassandra();
		if (null != client) {
			client.set_keyspace(KEYSPACE);
			// ColumnPath path = new ColumnPath(COLUMNFAMILY);
			// path.setSuper_column(SUPER_COLUMN.getBytes());
			ColumnParent parent = new ColumnParent(COLUMNFAMILY);// ����column
			// parent.
			SlicePredicate predicate = new SlicePredicate();
			SliceRange sliceRange = new SliceRange(toByteBuffer(""),
					toByteBuffer(""), false, 10);
			predicate.setSlice_range(sliceRange);
			List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
			String studentId = "c";

			// key的获取最为关键

			for (int i = 0; i <= 100; i++) {
				String k = studentId + i;// key
				keys.add(toByteBuffer(k));
			}
			// 获取COLUMN 或者 SUPER_COLUMN下的所有元素：
			Map<ByteBuffer, List<ColumnOrSuperColumn>> res = client
					.multiget_slice(keys, parent, predicate,
							ConsistencyLevel.ONE);
			iteratorColumnOrSuperColumnMap(res);
		}
	}

	/*
	 * 处理ColumnOrSuperColumn MAP, 数据结构为： "  key : 元素为ColumnOrSuperColumn的List "
	 */
	public static void iteratorColumnOrSuperColumnMap(
			Map<ByteBuffer, List<ColumnOrSuperColumn>> res) throws IOException {
		// 有序输出
		TreeMap<ByteBuffer, List<ColumnOrSuperColumn>> t = new TreeMap<ByteBuffer, List<ColumnOrSuperColumn>>(
				res);
		Set<ByteBuffer> s = t.keySet();
		Iterator<ByteBuffer> its = s.iterator();
		while (its.hasNext()) {
			ByteBuffer itsk = its.next();
			List<ColumnOrSuperColumn> slt = t.get(itsk);
			// System.out.print(toString(itsk) + " ** ");
			iteratorColumnOrSuperColumnList(itsk, slt);
			// System.out.println();
		}
	}

	/*
	 * 处理 ColumnOrSuperColumnList
	 */
	public static void iteratorColumnOrSuperColumnList(ByteBuffer keyName,
			List<ColumnOrSuperColumn> tmp) throws UnsupportedEncodingException {
		if (null == tmp || tmp.size() == 0) {
			return;
		}
		//最外面的  key名
		String KeyName = toString(keyName);

		for (ColumnOrSuperColumn result : tmp) {
			// 解析key对应的column list中的Supercolumn
			if (result.isSetSuper_column()) {
				SuperColumn sc = result.super_column;
				//supercolumn 的key名
				String SuperColumnsKeyName = new String(sc.getName());
				Iterator<Column> cit = sc.getColumnsIterator();
				while (cit.hasNext()) {
					Column column = cit.next();
					parseSuperColumnSingleColumn(KeyName, SuperColumnsKeyName,
							column);
				}
			}
			// 解析key对应的column list中的column
			if (result.isSetColumn()) {
//				Column column = result.column;
//				column.getName();
//				parseSingleColumn(KeyName, column);
			}

		}
	}

	// 解析 SuperColumn中 单个 column元素 ,并存到 Mysql中对应的表中
	public static void parseSuperColumnSingleColumn(String KeyName,
			String SuperKeyName, Column column)
			throws UnsupportedEncodingException {
		if (null == column) {
			return;
		}
		GeneralService generalService = (GeneralService) SpringUtil
				.getBean("generalService");
		String name = toString(column.name);
		String value = toString(column.value);
		// DataUtil.insertData(name,KeyName,SuperKeyName,value);
		// 数据库中字段分别为keyName，
		// superkeyName，不定字段name（上面column的name值，对应的值为上面的value）；
		// insert into scorm_stu_sco(keyName,superkeyName,name)
		// values(KeyName,SuperKeyName,value)
		
		//String insertSql = "insert into scorm_stu_sco(keyName,superKeyName,"+name+") values("+KeyName+","+SuperKeyName+","+value+")";
		
	 	 //String updateSql ="update scorm_stu_sco set name="+value+" where id='"+item[10]+"'";
		
//		try {
//		//	generalService.executeBySQL(updateSql);
//		} catch (EntityException e) {
//			e.printStackTrace();
//		}
		System.out.print(KeyName + "  " + SuperKeyName + "  " + name + ":"
				+ value);
		System.out.println();
	}

	// 解析 columns中的column元素
	public static void parseSingleColumn(String keyName, Column column)
			throws UnsupportedEncodingException {
		if (null == column) {
			return;
		}
		String name = toString(column.name);
		String value = toString(column.value);
		System.out.print(keyName + " " + name + ":" + value);
		System.out.print("\r\n");
	}

	// 最初的 解析 单个 column元素
	public static void parseSingleColumn(Column column)
			throws UnsupportedEncodingException {
		if (null == column) {
			return;
		}
		String name = toString(column.name);
		String value = toString(column.value);
		System.out.print(name + ":" + value);
		System.out.print(" ## ");
	}

	/*
	 * String to bytebuffer
	 */
	public static ByteBuffer toByteBuffer(String value)
			throws UnsupportedEncodingException {
		return ByteBuffer.wrap(value.getBytes("UTF-8"));
	}

	/*
	 * bytebuffer to String
	 */

	public static String toString(ByteBuffer buffer)
			throws UnsupportedEncodingException {
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return new String(bytes, "UTF-8");
	}

	public static void main(String[] args) throws TException,
			InvalidRequestException, UnavailableException, NotFoundException,
			TimedOutException, IOException {
		process();
	}

}
