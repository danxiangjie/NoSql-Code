package com.canssandra;

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
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/*
 * author wenbin
 * date 2013-05-01
 */
public class parsingCassandraDataUtil {

	private static final String KEYSPACE = "demo";
	public static final String COLUMNFAMILY = "pt";
	// public static final String SUPER_COLUMN = "pt";
	private static Cassandra.Client client = null;

	public static void connectCassandra() throws TException,
			InvalidRequestException, UnavailableException,
			UnsupportedEncodingException, NotFoundException, TimedOutException {

		// 包装好的socket
		TTransport tr = new TFramedTransport(
				new TSocket("192.168.20.103", 9160));
		TProtocol proto = new TBinaryProtocol(tr);
		client = new Cassandra.Client(proto);
		tr.open();
		if (!tr.isOpen()) {
			System.out.println("failed to connect server!");
			return;
		} else {
			System.out.println("success to connect server!");
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
			ColumnParent parent = new ColumnParent(COLUMNFAMILY);// 表名，column
			SlicePredicate predicate = new SlicePredicate();
			SliceRange sliceRange = new SliceRange(toByteBuffer(""),
					toByteBuffer(""), false, 10);
			predicate.setSlice_range(sliceRange);
			List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
			String key_user_id = "a";
			
			for (int i = 0; i < 100000; i++) {
				String k = key_user_id + i;// key
				keys.add(toByteBuffer(k));
			}
			
			// 获取SUPER_COLUMN下的所有元素：---关键：keys的准备与设置
			Map<ByteBuffer, List<ColumnOrSuperColumn>> res = client
					.multiget_slice(keys, parent, predicate,
							ConsistencyLevel.ONE);
			iteratorColumnOrSuperColumnMap(res);
		}
	}

	/*
	 * 处理ColumnOrSuperColumn MAP
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
			System.out.print(toString(itsk) + " ** ");
			iteratorColumnOrSuperColumnList(slt);
			System.out.println();
		}
	}

	/*
	 * 遍历返回的ColumnOrSuperColumn List,插入带数据库中
	 */
	public static void iteratorColumnOrSuperColumnList(
			List<ColumnOrSuperColumn> tmp) throws UnsupportedEncodingException {
		if (null == tmp || tmp.size() == 0) {
			return;
		}
		for (ColumnOrSuperColumn result : tmp) {
			Column column = result.column;
			String cname = toString(column.name);
			String cvalue = toString(column.value);
			System.out.print(cname + ":" + cvalue);
			System.out.print(" ** ");
		}
	}

	/*
	 * String转换为bytebuffer，以便插入cassandra
	 */
	public static ByteBuffer toByteBuffer(String value)
			throws UnsupportedEncodingException {
		return ByteBuffer.wrap(value.getBytes("UTF-8"));
	}

	/*
	 * bytebuffer转换为String
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
