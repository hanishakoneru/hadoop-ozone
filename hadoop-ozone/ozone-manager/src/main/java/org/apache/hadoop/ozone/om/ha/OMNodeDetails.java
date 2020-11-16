/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.ha;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneIllegalArgumentException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMNodeInfo;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_DB_CHECKPOINT_REQUEST_FLUSH;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_KEY;

/**
 * This class stores OM node details.
 */
public final class OMNodeDetails {
  private String omServiceId;
  private String omNodeId;
  private InetSocketAddress rpcAddress;
  private int rpcPort;
  private int ratisPort;
  private String httpAddress;
  private String httpsAddress;

  /**
   * Constructs OMNodeDetails object.
   */
  private OMNodeDetails(String serviceId, String nodeId,
      InetSocketAddress rpcAddr, int rpcPort, int ratisPort,
      String httpAddress, String httpsAddress) {
    this.omServiceId = serviceId;
    this.omNodeId = nodeId;
    this.rpcAddress = rpcAddr;
    this.rpcPort = rpcPort;
    this.ratisPort = ratisPort;
    this.httpAddress = httpAddress;
    this.httpsAddress = httpsAddress;
  }

  @Override
  public String toString() {
    return "OMNodeDetails["
        + "omServiceId=" + omServiceId +
        ", omNodeId=" + omNodeId +
        ", rpcAddress=" + rpcAddress +
        ", rpcPort=" + rpcPort +
        ", ratisPort=" + ratisPort +
        ", httpAddress=" + httpAddress +
        ", httpsAddress=" + httpsAddress +
        "]";
  }

  /**
   * Builder class for OMNodeDetails.
   */
  public static class Builder {
    private String omServiceId;
    private String omNodeId;
    private InetSocketAddress rpcAddress;
    private int rpcPort;
    private int ratisPort;
    private String httpAddr;
    private String httpsAddr;

    public Builder setRpcAddress(InetSocketAddress rpcAddr) {
      this.rpcAddress = rpcAddr;
      this.rpcPort = rpcAddress.getPort();
      return this;
    }

    public Builder setRatisPort(int port) {
      this.ratisPort = port;
      return this;
    }

    public Builder setOMServiceId(String serviceId) {
      this.omServiceId = serviceId;
      return this;
    }

    public Builder setOMNodeId(String nodeId) {
      this.omNodeId = nodeId;
      return this;
    }

    public Builder setHttpAddress(String httpAddress) {
      this.httpAddr = httpAddress;
      return this;
    }

    public Builder setHttpsAddress(String httpsAddress) {
      this.httpsAddr = httpsAddress;
      return this;
    }

    public OMNodeDetails build() {
      return new OMNodeDetails(omServiceId, omNodeId, rpcAddress, rpcPort,
          ratisPort, httpAddr, httpsAddr);
    }
  }

  public String getOMServiceId() {
    return omServiceId;
  }

  public String getOMNodeId() {
    return omNodeId;
  }

  public InetSocketAddress getRpcAddress() {
    return rpcAddress;
  }

  public boolean isHostUnresolved() {
    return rpcAddress.isUnresolved();
  }

  public InetAddress getInetAddress() {
    return rpcAddress.getAddress();
  }

  public String getHostName() {
    return rpcAddress.getHostName();
  }

  public String getRatisHostPortStr() {
    StringBuilder hostPort = new StringBuilder();
    hostPort.append(getHostName())
        .append(":")
        .append(ratisPort);
    return hostPort.toString();
  }

  public int getRatisPort() {
    return ratisPort;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public String getRpcAddressString() {
    return NetUtils.getHostPortString(rpcAddress);
  }

  public String getOMDBCheckpointEnpointUrl(boolean isHttpPolicy) {
    if (isHttpPolicy) {
      if (StringUtils.isNotEmpty(httpAddress)) {
        return "http://" + httpAddress + OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT
            + "?" + OZONE_DB_CHECKPOINT_REQUEST_FLUSH + "=true";
      }
    } else {
      if (StringUtils.isNotEmpty(httpsAddress)) {
        return "https://" + httpsAddress + OZONE_OM_DB_CHECKPOINT_HTTP_ENDPOINT
            + "?" + OZONE_DB_CHECKPOINT_REQUEST_FLUSH + "=true";
      }
    }
    return null;
  }

  public static OMNodeDetails getFromProto(OMNodeInfo omNodeInfo) {
    InetSocketAddress rpcAddr = NetUtils.createSocketAddrForHost(
        omNodeInfo.getHostAddress(), omNodeInfo.getRpcPort());
    return new OMNodeDetails.Builder()
        .setOMNodeId(omNodeInfo.getNodeId())
        .setRpcAddress(rpcAddr)
        .setRatisPort(omNodeInfo.getRatisPort())
        .setHttpAddress(omNodeInfo.getHttpAddr())
        .setHttpsAddress(omNodeInfo.getHttpsAddr())
        .setOMServiceId(omNodeInfo.getServiceId())
        .build();
  }

  public static OMNodeDetails getOMNodeDetailsFromConf(OzoneConfiguration conf,
      String omServiceId, String omNodeId) {
    String rpcAddrKey = OmUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
        omServiceId, omNodeId);
    String rpcAddrStr = OmUtils.getOmRpcAddress(conf, rpcAddrKey);
    if (rpcAddrStr == null || rpcAddrStr.isEmpty()) {
      throw new OzoneIllegalArgumentException("There is no OM configuration " +
          "for node ID " + omNodeId + " in ozone-site.xml.");
    }

    String ratisPortKey = OmUtils.addKeySuffixes(OZONE_OM_RATIS_PORT_KEY,
        omServiceId, omNodeId);
    int ratisPort = conf.getInt(ratisPortKey, OZONE_OM_RATIS_PORT_DEFAULT);

    InetSocketAddress omRpcAddress = null;
    try {
      omRpcAddress = NetUtils.createSocketAddr(rpcAddrStr);
    } catch (Exception e) {
      throw new OzoneIllegalArgumentException("Couldn't create socket address" +
          " for OM " + omNodeId + " at " + rpcAddrStr, e);
    }

    String httpAddr = OmUtils.getHttpAddressForOMPeerNode(conf,
        omServiceId, omNodeId, omRpcAddress.getHostName());
    String httpsAddr = OmUtils.getHttpsAddressForOMPeerNode(conf,
        omServiceId, omNodeId, omRpcAddress.getHostName());

    return new OMNodeDetails.Builder()
        .setOMNodeId(omNodeId)
        .setRpcAddress(omRpcAddress)
        .setRatisPort(ratisPort)
        .setHttpAddress(httpAddr)
        .setHttpsAddress(httpsAddr)
        .setOMServiceId(omServiceId)
        .build();
  }
}
