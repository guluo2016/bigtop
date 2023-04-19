/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bigtop.itest.hbase.util;

import java.io.IOException;
import java.security.MessageDigest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;

public class HBaseTestUtil {

  private static final String HADOOP_CONF_DIR = System.getenv("HADOOP_CONF_DIR");
  private static final String HBASE_CONF_DIR = System.getenv("HBASE_CONF_DIR");

  public static int BLOCKSIZE = 64 * 1024;
  public static String COMPRESSION =
      Compression.Algorithm.NONE.getName();

  private static String getTestPrefix() {
    return String.valueOf(System.currentTimeMillis());
  }

  public static byte[] getTestTableName(String testName) {
    return Bytes.toBytes(testName + "_" + getTestPrefix());
  }

  public static HTableDescriptor createTestTableDescriptor(String testName,
                                                           byte[] familyName) {
    byte[] tableName = getTestTableName(testName);
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(familyName));
    return htd;
  }

  public static Connection createConnection() {
    Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path(HBASE_CONF_DIR, "hbase-site.xml"));
    conf.addResource(new Path(HADOOP_CONF_DIR, "core-site.xml"));
    conf.addResource(new Path(HADOOP_CONF_DIR, "hdfs-site.xml"));

    Connection conn  = null;
    try {
      conn = ConnectionFactory.createConnection(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return conn;
  }

  public static Admin getAdmin(Connection conn) throws IOException{
    if (conn == null) {
      throw new IOException("Connection must not be null.");
    }

    Admin admin = null;
    try {
      admin = conn.getAdmin();
    } catch (IOException e) {
      throw new IOException("Create Admin failed.");
    }
    return admin;
  }

  public static FileSystem getClusterFileSystem() throws IOException {
    return FileSystem.get(new Configuration());
  }

  public static Path getMROutputDir(String testName) throws IOException {
    Path p = new Path(testName + "_" + getTestPrefix());
    return p.makeQualified(getClusterFileSystem());
  }

  /**
   * Create an HFile with the given number of rows between a given
   * start key and end key.
   */
  public static void createHFile(
      Configuration conf,
      FileSystem fs, Path path,
      byte[] family, byte[] qualifier,
      byte[] startKey, byte[] endKey, int numRows) throws IOException {
    HFile.WriterFactory wf = HFile.getWriterFactory(conf, new CacheConfig(conf));
    HFileContext hFileContext = new HFileContext();
    wf.withFileContext(hFileContext);
    wf.withComparator(KeyValue.COMPARATOR);
    wf.withPath(fs, path);
    HFile.Writer writer = wf.create();
    long now = System.currentTimeMillis();
    try {
      // subtract 2 since iterateOnSplits doesn't include boundary keys
      for (byte[] key : Bytes.iterateOnSplits(startKey, endKey, numRows - 2)) {
        KeyValue kv = new KeyValue(key, family, qualifier, now, key);
        writer.append(kv);
      }
    } finally {
      writer.close();
    }
  }

  /**
   * Return the number of rows in the given table.
   */
  public static int countRows(final HTable table) throws IOException {
    Scan scan = new Scan();
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (@SuppressWarnings("unused") Result res : results) {
      count++;
    }
    results.close();
    return count;
  }

  /**
   * Return an md5 digest of the entire contents of a table.
   */
  public static String checksumRows(final HTable table) throws Exception {
    Scan scan = new Scan();
    ResultScanner results = table.getScanner(scan);
    MessageDigest digest = MessageDigest.getInstance("MD5");
    for (Result res : results) {
      digest.update(res.getRow());
    }
    results.close();
    return digest.toString();
  }
}
