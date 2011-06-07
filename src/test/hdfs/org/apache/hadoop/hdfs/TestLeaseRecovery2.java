/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLeaseRecovery2 {
  
  public static final Log LOG = LogFactory.getLog(TestLeaseRecovery2.class);
  
  {
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
  }

  static final private long BLOCK_SIZE = 1024;
  static final private int FILE_SIZE = (int)BLOCK_SIZE*2;
  static final short REPLICATION_NUM = (short)3;
  static byte[] buffer = new byte[FILE_SIZE];
  
  static private String fakeUsername = "fakeUser1";
  static private String fakeGroup = "supergroup";

  static private MiniDFSCluster cluster;
  static private DistributedFileSystem dfs;
  final static private Configuration conf = new HdfsConfiguration();
  final static private int BUF_SIZE = conf.getInt("io.file.buffer.size", 4096);
  
  final static private long SHORT_LEASE_PERIOD = 1000L;
  final static private long LONG_LEASE_PERIOD = 60*60*SHORT_LEASE_PERIOD;
  
  /** start a dfs cluster
   * 
   * @throws IOException
   */
  @BeforeClass
  public static void startUp() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5).build();
    cluster.waitActive();
    dfs = (DistributedFileSystem)cluster.getFileSystem();
  }
  
  /**
   * stop the cluster
   * @throws IOException
   */
  @AfterClass
  public static void tearDown() throws IOException {
    IOUtils.closeStream(dfs);
    if (cluster != null) {cluster.shutdown();}
  }
  
  /**
   * This test makes the client does not renew its lease and also
   * set the hard lease expiration period to be short 1s. Thus triggering
   * lease expiration to happen while the client is still alive.
   * 
   * The test makes sure that the lease recovery completes and the client
   * fails if it continues to write to the file.
   * 
   * @throws Exception
   */
  @Test
  public void testHardLeaseRecovery() throws Exception {
    //create a file
    String filestr = "/hardLeaseRecovery";
    AppendTestUtil.LOG.info("filestr=" + filestr);
    Path filepath = new Path(filestr);
    FSDataOutputStream stm = dfs.create(filepath, true,
        BUF_SIZE, REPLICATION_NUM, BLOCK_SIZE);
    assertTrue(dfs.dfs.exists(filestr));

    // write bytes into the file.
    int size = AppendTestUtil.nextInt(FILE_SIZE);
    AppendTestUtil.LOG.info("size=" + size);
    stm.write(buffer, 0, size);

    // hflush file
    AppendTestUtil.LOG.info("hflush");
    stm.hflush();
    
    // kill the lease renewal thread
    AppendTestUtil.LOG.info("leasechecker.interruptAndJoin()");
    dfs.dfs.leaserenewer.interruptAndJoin();

    // set the hard limit to be 1 second 
    cluster.setLeasePeriod(LONG_LEASE_PERIOD, SHORT_LEASE_PERIOD);
    
    // wait for lease recovery to complete
    LocatedBlocks locatedBlocks;
    do {
      Thread.sleep(SHORT_LEASE_PERIOD);
      locatedBlocks = DFSClient.callGetBlockLocations(dfs.dfs.namenode,
        filestr, 0L, size);
    } while (locatedBlocks.isUnderConstruction());
    assertEquals(size, locatedBlocks.getFileLength());

    // make sure that the writer thread gets killed
    try {
      stm.write('b');
      stm.close();
      fail("Writer thread should have been killed");
    } catch (IOException e) {
      e.printStackTrace();
    }      

    // verify data
    AppendTestUtil.LOG.info(
        "File size is good. Now validating sizes from datanodes...");
    AppendTestUtil.checkFullFile(dfs, filepath, size, buffer, filestr);
  }
  
  /**
   * This test makes the client does not renew its lease and also
   * set the soft lease expiration period to be short 1s. Thus triggering
   * soft lease expiration to happen immediately by having another client
   * trying to create the same file.
   * 
   * The test makes sure that the lease recovery completes.
   * 
   * @throws Exception
   */
  @Test
  public void testSoftLeaseRecovery() throws Exception {
    Map<String, String []> u2g_map = new HashMap<String, String []>(1);
    u2g_map.put(fakeUsername, new String[] {fakeGroup});
    DFSTestUtil.updateConfWithFakeGroupMapping(conf, u2g_map);

    //create a file
    // create a random file name
    String filestr = "/foo" + AppendTestUtil.nextInt();
    AppendTestUtil.LOG.info("filestr=" + filestr);
    Path filepath = new Path(filestr);
    FSDataOutputStream stm = dfs.create(filepath, true,
        BUF_SIZE, REPLICATION_NUM, BLOCK_SIZE);
    assertTrue(dfs.dfs.exists(filestr));

    // write random number of bytes into it.
    int size = AppendTestUtil.nextInt(FILE_SIZE);
    AppendTestUtil.LOG.info("size=" + size);
    stm.write(buffer, 0, size);

    // hflush file
    AppendTestUtil.LOG.info("hflush");
    stm.hflush();
    AppendTestUtil.LOG.info("leasechecker.interruptAndJoin()");
    dfs.dfs.leaserenewer.interruptAndJoin();

    // set the soft limit to be 1 second so that the
    // namenode triggers lease recovery on next attempt to write-for-open.
    cluster.setLeasePeriod(SHORT_LEASE_PERIOD, LONG_LEASE_PERIOD);

    // try to re-open the file before closing the previous handle. This
    // should fail but will trigger lease recovery.
    {
      UserGroupInformation ugi = 
        UserGroupInformation.createUserForTesting(fakeUsername, 
            new String [] { fakeGroup});

      FileSystem dfs2 = DFSTestUtil.getFileSystemAs(ugi, conf);

      boolean done = false;
      for(int i = 0; i < 10 && !done; i++) {
        AppendTestUtil.LOG.info("i=" + i);
        try {
          dfs2.create(filepath, false, BUF_SIZE, REPLICATION_NUM, BLOCK_SIZE);
          fail("Creation of an existing file should never succeed.");
        } catch (FileAlreadyExistsException ex) {
          done = true;
        } catch (AlreadyBeingCreatedException ex) {
          AppendTestUtil.LOG.info("GOOD! got " + ex.getMessage());
        } catch (IOException ioe) {
          AppendTestUtil.LOG.warn("UNEXPECTED IOException", ioe);
        }

        if (!done) {
          AppendTestUtil.LOG.info("sleep " + 5000 + "ms");
          try {Thread.sleep(5000);} catch (InterruptedException e) {}
        }
      }
      assertTrue(done);
    }

    AppendTestUtil.LOG.info("Lease for file " +  filepath + " is recovered. "
        + "Validating its contents now...");

    // verify that file-size matches
    long fileSize = dfs.getFileStatus(filepath).getLen();
    assertTrue("File should be " + size + " bytes, but is actually " +
        " found to be " + fileSize + " bytes", fileSize == size);

    // verify data
    AppendTestUtil.LOG.info("File size is good. " +
                     "Now validating data and sizes from datanodes...");
    AppendTestUtil.checkFullFile(dfs, filepath, size, buffer, filestr);
  }
  
  /**
   * This test makes it so the client does not renew its lease and also
   * set the hard lease expiration period to be short, thus triggering
   * lease expiration to happen while the client is still alive. The test
   * also causes the NN to restart after lease recovery has begun, but before
   * the DNs have completed the blocks. This test verifies that when the NN
   * comes back up, the client no longer holds the lease.
   * 
   * The test makes sure that the lease recovery completes and the client
   * fails if it continues to write to the file, even after NN restart.
   * 
   * @throws Exception
   */
  @Test
  public void testHardLeaseRecoveryAfterNameNodeRestart() throws Exception {
    hardLeaseRecoveryRestartHelper(false);
  }
  
  @Test
  public void testHardLeaseRecoveryWithRenameAfterNameNodeRestart()
      throws Exception {
    hardLeaseRecoveryRestartHelper(true);
  }
  
  public void hardLeaseRecoveryRestartHelper(boolean doRename)
      throws Exception {
    //create a file
    String fileStr = "/hardLeaseRecovery";
    AppendTestUtil.LOG.info("filestr=" + fileStr);
    Path filePath = new Path(fileStr);
    FSDataOutputStream stm = dfs.create(filePath, true,
        BUF_SIZE, REPLICATION_NUM, BLOCK_SIZE);
    assertTrue(dfs.dfs.exists(fileStr));

    // write bytes into the file.
    int size = AppendTestUtil.nextInt(FILE_SIZE);
    AppendTestUtil.LOG.info("size=" + size);
    stm.write(buffer, 0, size);
    
    String originalLeaseHolder = NameNodeAdapter.getLeaseHolderForPath(
        cluster.getNameNode(), fileStr);
    
    assertFalse("original lease holder should not be the NN",
        originalLeaseHolder.equals(HdfsConstants.NAMENODE_LEASE_HOLDER));

    // hflush file
    AppendTestUtil.LOG.info("hflush");
    stm.hflush();
    
    if (doRename) {
      fileStr += ".renamed";
      Path renamedPath = new Path(fileStr);
      assertTrue(dfs.rename(filePath, renamedPath));
      filePath = renamedPath;
    }
    
    // kill the lease renewal thread
    AppendTestUtil.LOG.info("leasechecker.interruptAndJoin()");
    dfs.dfs.leaserenewer.interruptAndJoin();
    
    // Make sure the DNs don't send a heartbeat for a while, so the blocks
    // won't actually get completed during lease recovery.
    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeAdapter.setHeartbeatsDisabledForTests(dn, true);
    }
    
    // set the hard limit to be 1 second 
    cluster.setLeasePeriod(LONG_LEASE_PERIOD, SHORT_LEASE_PERIOD);
    
    // Make sure lease recovery begins.
    Thread.sleep(HdfsConstants.NAMENODE_LEASE_RECHECK_INTERVAL * 2);
    
    assertEquals("lease holder should now be the NN", HdfsConstants.NAMENODE_LEASE_HOLDER,
        NameNodeAdapter.getLeaseHolderForPath(cluster.getNameNode(), fileStr));
    
    cluster.restartNameNode(false);
    
    assertEquals("lease holder should still be the NN after restart",
        HdfsConstants.NAMENODE_LEASE_HOLDER,
        NameNodeAdapter.getLeaseHolderForPath(cluster.getNameNode(), fileStr));
    
    // Let the DNs send heartbeats again.
    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeAdapter.setHeartbeatsDisabledForTests(dn, false);
    }

    cluster.waitActive();

    // set the hard limit to be 1 second, to initiate lease recovery. 
    cluster.setLeasePeriod(LONG_LEASE_PERIOD, SHORT_LEASE_PERIOD);
    
    // wait for lease recovery to complete
    LocatedBlocks locatedBlocks;
    do {
      Thread.sleep(SHORT_LEASE_PERIOD);
      locatedBlocks = DFSClient.callGetBlockLocations(dfs.dfs.namenode,
        fileStr, 0L, size);
    } while (locatedBlocks.isUnderConstruction());
    assertEquals(size, locatedBlocks.getFileLength());

    // make sure that the client can't write data anymore.
    stm.write('b');
    try {
      stm.hflush();
      fail("Should not be able to flush after we've lost the lease");
    } catch (IOException e) {
      LOG.info("Expceted exception on hflush", e);
    }
    
    try {
      stm.close();
      fail("Should not be able to close after we've lost the lease");
    } catch (IOException e) {
      LOG.info("Expected exception on close", e);
    }

    // verify data
    AppendTestUtil.LOG.info(
        "File size is good. Now validating sizes from datanodes...");
    AppendTestUtil.checkFullFile(dfs, filePath, size, buffer, fileStr);
  }
}
