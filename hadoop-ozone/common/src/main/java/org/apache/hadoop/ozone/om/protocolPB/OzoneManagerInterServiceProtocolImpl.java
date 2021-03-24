/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.protocolPB;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerInterServiceProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BootstrapOMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BootstrapOMResponse;
import org.apache.hadoop.security.UserGroupInformation;

public class OzoneManagerInterServiceProtocolImpl implements
    OzoneManagerInterServiceProtocol {

  private OmTransport transport;

  public OzoneManagerInterServiceProtocolImpl(ConfigurationSource source,
      UserGroupInformation ugi, String omServiceId) throws IOException {
    transport = OmTransportFactory.create(source, ugi, omServiceId);
  }

  @Override
  public void bootstrap(OMNodeDetails newOMNode) throws IOException {
    System.out.println("------ 1 OMISP bootstrap");
    BootstrapOMRequest bootstrapOMRequest = BootstrapOMRequest
        .newBuilder()
        .setNodeId(newOMNode.getOMNodeId())
        .setHostAddress(newOMNode.getHostAddress())
        .setRatisPort(newOMNode.getRatisPort())
        .build();

    BootstrapOMResponse bootstrapOMResponse =
        transport.bootstrap(bootstrapOMRequest);

    if (!bootstrapOMResponse.getSuccess()) {
      throw new IOException("Failed to Bootstrap OM. Error Code: " +
          bootstrapOMResponse.getErrorCode() + ", Error Message: " +
          bootstrapOMResponse.getErrorMsg());
    }
  }

  @Override
  public void close() throws IOException {
    transport.close();
  }

}
