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

package org.apache.hadoop.ozone.om.request.key;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.request.util.OmResponseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMReplayException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.response.key.OMKeyCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .Type;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.UniqueId;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles CreateKey request.
 */

public class OMKeyCreateRequest extends OMKeyRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyCreateRequest.class);

  public OMKeyCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    CreateKeyRequest createKeyRequest = getOmRequest().getCreateKeyRequest();
    Preconditions.checkNotNull(createKeyRequest);

    KeyArgs keyArgs = createKeyRequest.getKeyArgs();

    // We cannot allocate block for multipart upload part when
    // createMultipartKey is called, as we will not know type and factor with
    // which initiateMultipartUpload has started for this key. When
    // allocateBlock call happen's we shall know type and factor, as we set
    // the type and factor read from multipart table, and set the KeyInfo in
    // validateAndUpdateCache and return to the client. TODO: See if we can fix
    //  this. We do not call allocateBlock in openKey for multipart upload.

    CreateKeyRequest.Builder newCreateKeyRequest = null;
    KeyArgs.Builder newKeyArgs = null;
    if (!keyArgs.getIsMultipartKey()) {

      long scmBlockSize = ozoneManager.getScmBlockSize();

      // NOTE size of a key is not a hard limit on anything, it is a value that
      // client should expect, in terms of current size of key. If client sets
      // a value, then this value is used, otherwise, we allocate a single
      // block which is the current size, if read by the client.
      final long requestedSize = keyArgs.getDataSize() > 0 ?
          keyArgs.getDataSize() : scmBlockSize;

      boolean useRatis = ozoneManager.shouldUseRatis();

      HddsProtos.ReplicationFactor factor = keyArgs.getFactor();
      if (factor == null) {
        factor = useRatis ? HddsProtos.ReplicationFactor.THREE :
            HddsProtos.ReplicationFactor.ONE;
      }

      HddsProtos.ReplicationType type = keyArgs.getType();
      if (type == null) {
        type = useRatis ? HddsProtos.ReplicationType.RATIS :
            HddsProtos.ReplicationType.STAND_ALONE;
      }

      // TODO: Here we are allocating block with out any check for
      //  bucket/key/volume or not and also with out any authorization checks.
      //  As for a client for the first time this can be executed on any OM,
      //  till leader is identified.

      List< OmKeyLocationInfo > omKeyLocationInfoList =
          allocateBlock(ozoneManager.getScmClient(),
              ozoneManager.getBlockTokenSecretManager(), type, factor,
              new ExcludeList(), requestedSize, scmBlockSize,
              ozoneManager.getPreallocateBlocksMax(),
              ozoneManager.isGrpcBlockTokenEnabled(),
              ozoneManager.getOMNodeId());

      newKeyArgs = keyArgs.toBuilder().setModificationTime(Time.now())
              .setType(type).setFactor(factor)
              .setDataSize(requestedSize);

      newKeyArgs.addAllKeyLocations(omKeyLocationInfoList.stream()
          .map(OmKeyLocationInfo::getProtobuf).collect(Collectors.toList()));
    } else {
      newKeyArgs = keyArgs.toBuilder().setModificationTime(Time.now());
    }

    generateRequiredEncryptionInfo(keyArgs, newKeyArgs, ozoneManager);
    newCreateKeyRequest =
        createKeyRequest.toBuilder().setKeyArgs(newKeyArgs)
            .setClientID(UniqueId.next());

    return getOmRequest().toBuilder()
        .setCreateKeyRequest(newCreateKeyRequest).setUserInfo(getUserInfo())
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long trxnLogIndex, OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    CreateKeyRequest createKeyRequest = getOmRequest().getCreateKeyRequest();

    KeyArgs keyArgs = createKeyRequest.getKeyArgs();

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    LOG.info("----- OM-SM KeyCreate request {}, keyName: {}", trxnLogIndex, keyName);

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyAllocates();

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OmKeyInfo omKeyInfo = null;
    final List< OmKeyLocationInfo > locations = new ArrayList<>();

    boolean acquireLock = false;
    OMClientResponse omClientResponse = null;
    OMResponse.Builder omResponse = OmResponseUtil.getOMResponseBuilder(
        getOmRequest());
    IOException exception = null;
    Result result = null;
    try {
      // check Acl
      checkKeyAcls(ozoneManager, volumeName, bucketName, keyName,
          IAccessAuthorizer.ACLType.CREATE, OzoneObj.ResourceType.KEY);

      acquireLock = omMetadataManager.getLock().acquireWriteLock(BUCKET_LOCK,
          volumeName, bucketName);
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      //TODO: We can optimize this get here, if getKmsProvider is null, then
      // bucket encryptionInfo will be not set. If this assumption holds
      // true, we can avoid get from bucket table.

      // Check if Key already exists
      String dbKeyName = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName);
      OmKeyInfo dbKeyInfo =
          omMetadataManager.getKeyTable().getIfExist(dbKeyName);
      if (dbKeyInfo != null) {
        // Check if this transaction is a replay of ratis logs.
        // We check only the KeyTable here and not the OpenKeyTable. In case
        // this transaction is a replay but the transaction was not committed
        // to the KeyTable, then we recreate the key in OpenKey table. This is
        // okay as all the subsequent transactions would also be replayed and
        // the openKey table would eventually reach the same state.
        // The reason we do not check the OpenKey table is to avoid a DB read
        // in regular non-replay scenario.
        if (isReplay(ozoneManager, dbKeyInfo, trxnLogIndex)) {
          // Replay implies the response has already been returned to
          // the client. So take no further action and return a dummy
          // OMClientResponse.
          throw new OMReplayException();
        }
      }

      OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(
          omMetadataManager.getBucketKey(volumeName, bucketName));

      omKeyInfo = prepareKeyInfo(omMetadataManager, keyArgs, dbKeyInfo,
          keyArgs.getDataSize(), locations,  getFileEncryptionInfo(keyArgs),
          ozoneManager.getPrefixManager(), bucketInfo, trxnLogIndex,
          ozoneManager.isRatisEnabled());

      long openVersion = omKeyInfo.getLatestVersionLocations().getVersion();
      long clientID = createKeyRequest.getClientID();
      String dbOpenKeyName = omMetadataManager.getOpenKey(volumeName,
          bucketName, keyName, clientID);

      // Append new blocks
      omKeyInfo.appendNewBlocks(keyArgs.getKeyLocationsList().stream()
          .map(OmKeyLocationInfo::getFromProtobuf)
          .collect(Collectors.toList()), false);

      // Add to cache entry can be done outside of lock for this openKey.
      // Even if bucket gets deleted, when commitKey we shall identify if
      // bucket gets deleted.
      omMetadataManager.getOpenKeyTable().addCacheEntry(
          new CacheKey<>(dbOpenKeyName),
          new CacheValue<>(Optional.of(omKeyInfo), trxnLogIndex));

      // Prepare response
      omResponse.setCreateKeyResponse(CreateKeyResponse.newBuilder()
          .setKeyInfo(omKeyInfo.getProtobuf())
          .setID(clientID)
          .setOpenVersion(openVersion).build())
          .setCmdType(Type.CreateKey);
      omClientResponse = new OMKeyCreateResponse(omResponse.build(),
          omKeyInfo, null, clientID);

      LOG.info("----- OM-SM KeyCreate request {} success, keyName: {}", trxnLogIndex,
          keyName);

      result = Result.SUCCESS;
    } catch (IOException ex) {
      if (ex instanceof OMReplayException) {
        result = Result.REPLAY;
        omClientResponse = new OMKeyCreateResponse(createReplayOMResponse(
            omResponse));
      } else {
        result = Result.FAILURE;
        exception = ex;
        omMetrics.incNumKeyAllocateFails();
        omResponse.setCmdType(Type.CreateKey);
        omClientResponse = new OMKeyCreateResponse(createErrorOMResponse(
            omResponse, exception));
      }
    } finally {
      addResponseToDoubleBuffer(trxnLogIndex, omClientResponse,
          omDoubleBufferHelper);
      if (acquireLock) {
        omMetadataManager.getLock().releaseWriteLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    // Audit Log outside the lock
    if (result != Result.REPLAY) {
      Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
      auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(
          OMAction.ALLOCATE_KEY, auditMap, exception,
          getOmRequest().getUserInfo()));
    }

    switch (result) {
    case SUCCESS:
      LOG.debug("Key created. Volume:{}, Bucket:{}, Key:{}", volumeName,
          bucketName, keyName);
      break;
    case REPLAY:
      LOG.debug("Replayed Transaction {} ignored. Request: {}", trxnLogIndex,
          createKeyRequest);
      break;
    case FAILURE:
      LOG.error("Key creation failed. Volume:{}, Bucket:{}, Key{}. " +
              "Exception:{}", volumeName, bucketName, keyName, exception);
      break;
    default:
      LOG.error("Unrecognized Result for OMKeyCreateRequest: {}",
          createKeyRequest);
    }

    return omClientResponse;
  }

}
