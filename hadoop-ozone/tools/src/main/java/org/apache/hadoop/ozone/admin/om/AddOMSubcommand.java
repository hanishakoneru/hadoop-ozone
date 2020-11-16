/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.admin.om;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMNodeInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import org.apache.hadoop.util.StringUtils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_PORT_KEY;

/**
 * Handler of om roles command.
 */
@CommandLine.Command(
    name = "addom", aliases = "addoms, addnewom, addnewoms",
    description = "Adds newly Bootstrapped OM(s) to the cluster. " +
        "\nNote - New OM(s) should be started in bootstrap mode before " +
        "running this command." +
        "\nNote - Configuration files of all existing " +
        "OM(s) MUST be updated with information about the new OM(s) (" +
        OZONE_OM_NODES_KEY + ", " + OZONE_OM_ADDRESS_KEY + ", " +
        OZONE_OM_HTTP_ADDRESS_KEY + ", " + OZONE_OM_HTTPS_ADDRESS_KEY + ", " +
        OZONE_OM_RATIS_PORT_KEY + ").",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class AddOMSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(names = {"-id", "--service-id"},
      description = "OM Service ID",
      required = true)
  private String omServiceId;

  @CommandLine.Option(names = {"-nodeid", "-nodeids", "--new-node-ids"},
      description = "A comma separated list of node IDs of the new OM(s). ",
      required = true)
  private String newNodeIds;

  private OzoneManagerProtocol ozoneManagerClient;

  @Override
  public Void call() throws Exception {
    boolean success = false;
    try {
      ozoneManagerClient =  parent.createOmClient(omServiceId);

      List<OMNodeInfo> newOMNodeInfos = getNewNodeInfos();

      if (!newOMNodeInfos.isEmpty()) {
        success = ozoneManagerClient.bootstrap(newOMNodeInfos);
      }
    } catch (OzoneClientException ex) {
      System.out.printf("Error: %s", ex.getMessage());
    } finally {
      if (ozoneManagerClient != null) {
        ozoneManagerClient.close();
      }
    }
    if (success) {
      System.out.println("Successfully Added new OM(s): " + newNodeIds);
    } else {
      System.out.println("Failed to Bootstrap new OM(s): " + newNodeIds);
    }
    return null;
  }

  private List<OMNodeInfo> getNewNodeInfos() throws IOException {
    OzoneConfiguration conf = parent.getParent().getOzoneConf();

    Collection<String> newNodeIdsList = StringUtils.getTrimmedStringCollection(
        newNodeIds);
    ArrayList<OMNodeInfo> newOMNodeInfosList = new ArrayList<>();

    for (String newNodeId : newNodeIdsList) {
      String rpcAddrKey = OmUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
          omServiceId, newNodeId);
      String rpcAddrStr = OmUtils.getOmRpcAddress(conf, rpcAddrKey);
      if (rpcAddrStr == null || rpcAddrStr.isEmpty()) {
        System.out.println("Error: There is no OM corresponding to " +
            newNodeId + "in the configuration. Set the new OM node's RPC " +
            "address via the key " + rpcAddrKey + ". Also update " +
            OZONE_OM_NODES_KEY + "and set other node properties - " +
            OZONE_OM_HTTP_ADDRESS_KEY + ", " + OZONE_OM_HTTPS_ADDRESS_KEY +
            "and " + OZONE_OM_RATIS_PORT_KEY + "if not using the default " +
            "ports. Update all the OM's with these configurations.");
        return null;
      }

      String ratisPortKey = OmUtils.addKeySuffixes(OZONE_OM_RATIS_PORT_KEY,
          omServiceId, newNodeId);
      int ratisPort = conf.getInt(ratisPortKey, OZONE_OM_RATIS_PORT_DEFAULT);

      InetSocketAddress newNodeRpcAddress = null;
      try {
        newNodeRpcAddress = NetUtils.createSocketAddr(rpcAddrStr);
      } catch (Exception e) {
        System.out.println("Error: Couldn't create socket address for OM " +
            newNodeId + " at " + rpcAddrStr);
        return null;
      }

      // Check that the new OM is not already part of the OM ring.
      for (ServiceInfo serviceInfo : ozoneManagerClient.getServiceList()) {
        OMRoleInfo omRoleInfo = serviceInfo.getOmRoleInfo();
        if (omRoleInfo != null &&
            serviceInfo.getNodeType() == HddsProtos.NodeType.OM) {
          if (omRoleInfo.getNodeId().equals(newNodeId)) {
            System.out.println("Error: New node cannot have the same nodeId " +
                "as an existing OM node.");
            return null;
          }
          // TODO: Check that the new node address is not the same as an
          //  existing OM node.
        }
      }

      String httpAddr = OmUtils.getHttpAddressForOMPeerNode(conf,
          omServiceId, newNodeId, newNodeRpcAddress.getHostName());
      String httpsAddr = OmUtils.getHttpsAddressForOMPeerNode(conf,
          omServiceId, newNodeId, newNodeRpcAddress.getHostName());

      newOMNodeInfosList.add(OMNodeInfo.newBuilder()
          .setNodeId(newNodeId)
          .setHostAddress(newNodeRpcAddress.getHostString())
          .setRpcPort(newNodeRpcAddress.getPort())
          .setRatisPort(ratisPort)
          .setHttpAddr(httpAddr)
          .setHttpsAddr(httpsAddr)
          .setServiceId(omServiceId)
          .build());
    }

    return newOMNodeInfosList;
  }
}
