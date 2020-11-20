package org.apache.hadoop.ozone.om;

import com.google.common.base.Supplier;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SERVER_ROLE_CHECK_INTERVAL_KEY;
import static org.apache.hadoop.ozone.om.TestOzoneManagerHA.createKey;

public class TestOzoneManagerBootstrap {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public Timeout timeout = new Timeout(500_000);

  private MiniOzoneHAClusterImpl cluster = null;
  private ObjectStore objectStore;
  private OzoneConfiguration conf;
  private final String clusterId = UUID.randomUUID().toString();
  private final String scmId = UUID.randomUUID().toString();
  private final String omServiceId = "om-bootstrap";

  private static final int NUM_INITIAL_OMS = 3;
  private static final TimeDuration ROLE_CHECK_INTERVAL =
      TimeDuration.valueOf(2, TimeUnit.SECONDS);

  private static final String volumeName;
  private static final String bucketName;
  private String keyName;
  private long lastTransactionIndex;

  static {
    volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
  }

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_OM_RATIS_SERVER_ROLE_CHECK_INTERVAL_KEY,
        ROLE_CHECK_INTERVAL.getDuration(), ROLE_CHECK_INTERVAL.getUnit());
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOMServiceId(omServiceId)
        .setNumOfOzoneManagers(NUM_INITIAL_OMS)
        .build();
    cluster.waitForClusterToBeReady();
    objectStore = OzoneClientFactory.getRpcClient(omServiceId, conf)
        .getObjectStore();

    // Perform some transactions
    objectStore.createVolume(volumeName);
    OzoneVolume volume = objectStore.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    keyName = createKey(bucket);

    lastTransactionIndex = cluster.getOMLeader().getRatisSnapshotIndex();
  }

  @After
  public void shutdown() throws Exception {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    System.out.println("----- RaftServerImpl shutdown called:");
    for(StackTraceElement element : stackTrace) {
      System.out.println("     " + element.getClassName() + " - " + element.getMethodName() +
          " - " +
          element.getLineNumber());
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void assertNewOMExistsInPeerList(String nodeId) throws Exception {
    for (OzoneManager om : cluster.getOzoneManagersList()) {
      Assert.assertTrue("New OM node " + nodeId + " not present in Peer list",
          om.doesPeerExist(nodeId));
    }
    OzoneManager newOM = cluster.getOzoneManager(nodeId);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          return newOM.getRatisSnapshotIndex() >= lastTransactionIndex;
        } catch (IOException e) {
          return false;
        }
      }
    }, 100, 10000);

    // Check Ratis Dir for log files
    File[] logFiles = getRatisLogFiles(newOM);
    Assert.assertTrue("There are no ratis logs in new OM ",
        logFiles.length > 0);
  }

  private File[] getRatisLogFiles(OzoneManager om) {
    OzoneManagerRatisServer newOMRatisServer = om.getOmRatisServer();
    File ratisDir = new File(newOMRatisServer.getRatisStorageDir(),
        newOMRatisServer.getRaftGroupId());
    File ratisLogDir = new File(ratisDir, Storage.STORAGE_DIR_CURRENT);
    return ratisLogDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().startsWith("log");
      }
    });
  }

  /**
   * Add 1 new OM to cluster.
   * @throws Exception
   */
  @Test
  public void testBootstrapOneNewOM() throws Exception {
    List<String> newOMNodeIds = cluster.bootstrapOzoneManagers(1);
    assertNewOMExistsInPeerList(newOMNodeIds.get(0));
  }

  /**
   * Add 2 new OMs to cluster.
   * @throws Exception
   */
  @Test
  public void testBootstrapTwoNewOMs() throws Exception {
    List<String> newOMNodeIds = cluster.bootstrapOzoneManagers(2);
    assertNewOMExistsInPeerList(newOMNodeIds.get(0));
    assertNewOMExistsInPeerList(newOMNodeIds.get(1));
  }

  /**
   * Add 2 OMs in two separate AddOM calls.
   * @throws Exception
   */
  @Test
  public void testBootstrapTwoOMsOneAfterAnother() throws Exception {
    List<String> newOMNodeId1 = cluster.bootstrapOzoneManagers(1);
    List<String> newOMNodeId2 = cluster.bootstrapOzoneManagers(1);
    assertNewOMExistsInPeerList(newOMNodeId1.get(0));
    assertNewOMExistsInPeerList(newOMNodeId2.get(0));
  }
}
