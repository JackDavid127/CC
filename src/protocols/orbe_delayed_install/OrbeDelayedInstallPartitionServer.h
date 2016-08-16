//
//  OrbeDelayedInstallPartitionServer.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>
#include <algorithm>
#include <deque>
#include "Timing.h"
#include "OrbeDelayedInstall.h"
#include "OrbeDelayedInstallPartitionServerGetBeforePutHandler.h"
#include "OrbeDelayedInstallPartitionServerGetBeforePutContext.h"
#include "OrbeDelayedInstallPendingTimestampQuery.h"

#include "VersionVectorString.h"

//#define OrbeDelayedInstallPartitionServer_DEBUG_LOGGING

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            template<typename TProtocolTraits>
            class OrbeDelayedInstallPartitionServer : private boost::noncopyable
            {
                typedef OrbeDelayedInstallPartitionServer<TProtocolTraits> TSelf;
                static const size_t NumPartitions = TProtocolTraits::NumPartitions;
                static const size_t NumReplicas = TProtocolTraits::NumReplicas;
                
                typedef typename TProtocolTraits::TVersionVector TVersionVector;
                typedef typename TProtocolTraits::TClientId TClientId;
                typedef typename TProtocolTraits::TClientDependencyTimestamp TClientDependencyTimestamp;
                typedef typename TProtocolTraits::TClientDependencyPartition TClientDependencyPartition;
                typedef typename TProtocolTraits::TItemDependencyTimestamp TItemDependencyTimestamp;
                typedef typename TProtocolTraits::TItemDependencyReplica TItemDependencyReplica;
                typedef typename TProtocolTraits::TNetworkExchange TNetworkExchange;
                typedef typename TProtocolTraits::TBroadcaster TBroadcaster;
                typedef typename TProtocolTraits::TStorageKey TStorageKey;
                typedef typename TProtocolTraits::TSerializer TSerializer;
                typedef typename TProtocolTraits::TSerializedDataType TSerializedDataType;
                typedef typename TProtocolTraits::TDeserializedValueType TDeserializedValueType;
                typedef typename TProtocolTraits::TClientInputValueType TClientInputValueType;
                typedef typename TProtocolTraits::TClientGetReplyValueType TClientGetReplyValueType;
                typedef typename TProtocolTraits::TClientPutReplyValueType TClientPutReplyValueType;
                typedef typename TProtocolTraits::TReplicationMessageType TReplicationMessageType;
                typedef typename TProtocolTraits::TStorageComponent TStorage;
                typedef typename TProtocolTraits::TMetrics TMetrics;
                typedef typename TProtocolTraits::TDelayTracking TDelayTracking;
                typedef typename TProtocolTraits::TLogger TLogger;
                typedef typename TProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                typedef typename TProtocolTraits::TReplicationHandler TReplicationHandler;
                typedef typename TProtocolTraits::TClientUpdateSpace TClientUpdateSpace;
                typedef typename TProtocolTraits::TReplicaUpdateSpace TReplicaUpdateSpace;
                typedef typename TVersionVector::TVersionVectorCounterType TVersionVectorCounterType;
                
                
                typedef OrbeDelayedInstallPendingLocalTimestampQuery<TProtocolTraits> TPendingLocalTimestampQuery;
                typedef OrbeDelayedInstallPendingGlobalTimestampQuery<TProtocolTraits> TPendingGlobalTimestampQuery;
                typedef std::vector<TPendingGlobalTimestampQuery> TPendingGlobalTimestampQueries;
                
            public:
                OrbeDelayedInstallPartitionServer(ReplicaIdType replicaId,
                                                  PartitionIdType partitionId,
                                                  TStorage& storage,
                                                  TNetworkExchange& networkExchange,
                                                  TBroadcaster& replicaPartitionsBroadcaster,
                                                  TSerializer& serializer,
                                                  TClientUpdateSpace& clientUpdateSpace,
                                                  TReplicaUpdateSpace& replicaUpdateSpace,
                                                  TMetrics& metrics,
                                                  TDelayTracking& delayTracking,
                                                  TLogger& logger)
                : m_replicaId(replicaId)
                , m_partitionId(partitionId)
                , m_storage(storage)
                , m_networkExchange(networkExchange)
                , m_replicaPartitionsBroadcaster(replicaPartitionsBroadcaster)
                , m_clientUpdateSpace(clientUpdateSpace)
                , m_replicaUpdateSpace(replicaUpdateSpace)
                , m_serializer(serializer)
                , m_metrics(metrics)
                , m_delayTracking(delayTracking)
                , m_logger(logger)
                , m_logicalVersionVector()
                , m_partitionServerGetBeforePutHandler(*this)
                , m_replicationHandler(logger, storage, serializer, *this, networkExchange, replicaUpdateSpace, delayTracking)
                , m_numPuts(0)
                , m_pendingTimestampQueries()
                , m_pendingTimestampQueriesReceived(0)
                , m_pendingTimestampQueriesSatisfied(0)
                , m_pendingGlobalTimestampQueries()
                , m_pendingGlobalTimestampQueriesReceived(0)
                , m_pendingGlobalTimestampQueriesSatisfied(0)
                {
                }
                
                ~OrbeDelayedInstallPartitionServer()
                {
                    
                }
                
                size_t PendingTimestampQueriesReceived() const
                {
                    return m_pendingTimestampQueriesReceived;
                }
                
                size_t PendingTimestampQueriesSatisfied() const
                {
                    return m_pendingTimestampQueriesSatisfied;
                }
                
                ReplicaIdType ReplicaId() const
                {
                    return m_replicaId;
                }
                
                PartitionIdType PartitionId() const
                {
                    return m_partitionId;
                }
                
                const TVersionVector& LogicalVersionVector() const
                {
                    return m_logicalVersionVector;
                }
                
                const TVersionVector& LogicalGlobalVersionVector() const
                {
                    return m_globalLogicalVersionVector;
                }
                
                size_t TotalUnappliedReplications() const
                {
                    return 0;
                }
                
                template<typename TGetCompletedHandler, typename TContext = NoContext>
                void Get(const TStorageKey& key,
                         const TClientId& clientId,
                         TGetCompletedHandler& getCompletedHandler,
                         TContext context = EmptyContext)
                {
                    //self.__num_gets.increment_and_get();
                    
                    TDeserializedValueType* out_pValue;
                    if(m_clientUpdateSpace.TryGet(clientId, key, out_pValue))
                    {
                        TDeserializedValueType& protocolItem = *out_pValue;
                        
                        TClientGetReplyValueType reply(protocolItem.Value(),
                                                       protocolItem.Timestamp(),
                                                       protocolItem.SourceReplicaId(),
                                                       protocolItem.LogicalTimestamp());
                        
                        DispatchToGetCompleteItemFoundHandler(getCompletedHandler, reply, context);
                        
                        return;
                    }
                    else
                    {
                        m_storage.Get(key, *this, getCompletedHandler, context);
                    }
                }
                
                template<typename TPutCompletedHandler, typename TContext = NoContext>
                void Put(const TStorageKey& key,
                         const TClientInputValueType& input,
                         const TClientId& clientId,
                         const TClientDependencyTimestamp& clientDependencyTimestamp,
                         const TClientDependencyPartition& clientDependencyPartition,
                         TPutCompletedHandler& putCompletedHandler,
                         TContext context = EmptyContext)
                {
                    ++m_numPuts;
                    
                    //m_logger.Log("partition server replica %d, partition %d\n", m_replicaId, m_partitionId);
                    boost::optional<TDeserializedValueType> value(boost::none);
                    
                    TDeserializedValueType* out_pValue;
                    if(m_clientUpdateSpace.TryGet(clientId, key, out_pValue))
                    {
                        value = *out_pValue;
                    }
                    
                    typedef OrbeDelayedInstallPartitionServerGetBeforePutContext<TProtocolTraits, TPutCompletedHandler, TContext> TGetBeforePutContext;
                    
                    auto additionalContext = TGetBeforePutContext(key,
                                                                  clientId,
                                                                  clientDependencyTimestamp,
                                                                  clientDependencyPartition,
                                                                  input,
                                                                  putCompletedHandler,
                                                                  context,
                                                                  value);
                    
                    m_storage.Get(key, m_partitionServerGetBeforePutHandler, putCompletedHandler, additionalContext);
                }
                
                template<typename TGetCompletedHandler, typename TContext = NoContext>
                void HandleGetCompleteItemFound(TGetCompletedHandler& getCompletedHandler,
                                                const TSerializedDataType& serializedValue,
                                                TContext context)
                {
                    //m_metrics.add_partition_server_get(m_partitionId, item)
                    
                    TDeserializedValueType protocolItem = m_serializer.Deserialize(serializedValue);
                    
                    TClientGetReplyValueType reply(protocolItem.Value(),
                                                   protocolItem.Timestamp(),
                                                   protocolItem.SourceReplicaId(),
                                                   protocolItem.LogicalTimestamp());
                    
                    DispatchToGetCompleteItemFoundHandler(getCompletedHandler, reply, context);
                }
                
                template<typename TGetCompletedHandler, typename TContext = NoContext>
                void HandleGetCompleteItemNotFound(TGetCompletedHandler& getCompletedHandler,
                                                   TContext context)
                {
                    getCompletedHandler.HandleGetCompleteItemNotFound(context);
                }
                
                template<typename TGetCompletedHandler>
                void HandleGetCompleteItemNotFound(TGetCompletedHandler& getCompletedHandler,
                                                   NoContext context)
                {
                    getCompletedHandler.HandleGetCompleteItemNotFound();
                }
                
                template<typename TPutCompletedHandler, typename TContext = NoContext>
                void HandlePutComplete(TPutCompletedHandler& putCompletedHandler,
                                       const TDeserializedValueType& protocolItem,
                                       TContext context)
                {
                    TReplicationMessageType message(ReplicationMessageFromProtocolItem(protocolItem, m_partitionId));
                    
                    m_replicaPartitionsBroadcaster.BroadcastReplicationToPartitionReplicas(message);
                    
                    TClientPutReplyValueType reply(protocolItem.Timestamp(),
                                                   protocolItem.SourceReplicaId(),
                                                   protocolItem.LogicalTimestamp());
                    
                    DispatchToPutCompleteHandler(putCompletedHandler, reply, context);
                }
                
                void ReceiveReplicateMessage(const TReplicationMessageType& replicateMessage)
                {
                    m_logger.Log("OrbeDelayedInstallPartitionServer::ReceiveReplicateMessage: Key: %d, LogicalTimestamp: %llu, SourceReplicaId:%d\n",
                                 replicateMessage.Value(),
                                 replicateMessage.LogicalTimestamp(),
                                 replicateMessage.SourceReplicaId());
                    
                    //self.__num_replications.increment_and_get()
                    
                    SR_ASSERT(replicateMessage.PartitionServerId() == m_partitionId);
                    SR_ASSERT(replicateMessage.SourceReplicaId() != m_replicaId);
                    
                    //self.__metrics.add_replicate_message_received_at_data_centre_replica(self.__replica_id, replicate_message.source_replica_id)
                    
                    //self.__metrics.add_partition_server_receives_replicate_message(self.__partition_id, self.__replica_id, replicate_message.source_replica_id)
                    
                    m_replicationHandler.ReceiveReplicateMessage(replicateMessage);
                }
                
                void ReceiveNeighbourTimestampDependencyCheckRequest(PartitionIdType senderPartitionId,
                                                                     const TClientDependencyTimestamp& dependencyTimestamp,
                                                                     ReplicaIdType sourceReplicaId)
                {
                    m_pendingTimestampQueriesReceived += 1;
                    
                    // one of our neighbouring partition servers within this replication site wants to know if our version vector satisfies
                    // one of its in-flight replication dependencies...
                    //
                    // we will call it back when this is the case
                    TPendingLocalTimestampQuery& query(m_pendingTimestampQueries[senderPartitionId]);
                    
                    query.SetPendingQuery(dependencyTimestamp,
                                          senderPartitionId,
                                          sourceReplicaId);
                    
                    TrySatisfyPendingTimestampQuery(query);
                }
                
                void ReceiveNeighbourTimestampDependencyCheckResponse(ReplicaIdType sourceReplicaId)
                {
                    m_replicationHandler.ReceiveNeighbourTimestampDependencyCheckResponse(sourceReplicaId);
                }
                
                void IncrementLogicalVersionVectorFromReplication(ReplicaIdType sourceReplicaId,
                                                                  TLogicalTimestamp expectedValue)
                {
                    SR_ASSERT(sourceReplicaId != m_replicaId);
                    SR_ASSERT((m_logicalVersionVector.GetValueForReplica(sourceReplicaId) + 1) == (expectedValue));
                    m_logicalVersionVector.IncrementForReplica(sourceReplicaId);
                    //printf("IncrementLogicalVersionVectorFromReplication:: %s\n", ToString(m_logicalVersionVector).c_str());
                }
                
                void IncrementGlobalLogicalVersionVector(ReplicaIdType sourceReplicaId,
                                                         TLogicalTimestamp expectedValue)
                {
                    //SR_ASSERT(sourceReplicaId != m_replicaId);
                    SR_ASSERT((m_globalLogicalVersionVector.GetValueForReplica(sourceReplicaId) + 1) == (expectedValue));
                    m_globalLogicalVersionVector.IncrementForReplica(sourceReplicaId);
                    //printf("IncrementLogicalVersionVectorFromReplication:: %s\n", ToString(m_logicalVersionVector).c_str());
                }
                
                void TrySatisfyPendingLocalTimestampQueries()
                {
                    for(size_t i = 0; i < NumPartitions; ++ i)
                    {
                        auto& query(m_pendingTimestampQueries[i]);
                        
                        if(query.IsPending())
                        {
                            TrySatisfyPendingTimestampQuery(query);
                        }
                    }
                }
                
                void TrySatisfyPendingGlobalTimestampQueries()
                {
                    const auto valueCount(m_pendingGlobalTimestampQueries.size());
                    
                    for(auto i = 0; i < valueCount; ++ i)
                    {
                        const auto valueIndex = (valueCount - 1) - i;
                        auto& query(m_pendingGlobalTimestampQueries[valueIndex]);
                        
                        SR_ASSERT(query.IsPending());
                        
                        TrySatisfyPendingTimestampQuery(query);
                        
                        if(!query.IsPending())
                        {
                            std::swap(query, m_pendingGlobalTimestampQueries.back());
                            m_pendingGlobalTimestampQueries.pop_back();
                        }
                    }
                }
                
                void ReceiveNeighbourGlobalTimestampDependencyCheckRequest(PartitionIdType senderPartitionId,
                                                                           const TClientDependencyTimestamp& dependencyTimestamp,
                                                                           ReplicaIdType sourceReplicaId)
                {
                    m_pendingGlobalTimestampQueriesReceived += 1;
                    
                    TPendingGlobalTimestampQuery query;
                    
                    query.SetPendingQuery(dependencyTimestamp,
                                          senderPartitionId,
                                          sourceReplicaId);
                    
                    TrySatisfyPendingTimestampQuery(query);
                    
                    if(query.IsPending())
                    {
                        // not satisfied
                        m_pendingGlobalTimestampQueries.push_back(query);
                    }
                }
                
                void ReceiveNeighbourGlobalTimestampDependencyCheckResponse(ReplicaIdType sourceReplicaId)
                {
                    m_replicationHandler.ReceiveNeighbourGlobalTimestampDependencyCheckResponse(sourceReplicaId);
                }
                
                template<typename TCompletionHandler, typename TPutCompletedHandler, typename TContext>
                void HandlePrePutGetCompletionItemFound(TCompletionHandler& completedHandler,
                                                        const TSerializedDataType& serializedValue,
                                                        const OrbeDelayedInstallPartitionServerGetBeforePutContext<TProtocolTraits, TPutCompletedHandler, TContext>& getBeforePutContext)
                {
                    TItemDependencyTimestamp itemDependencyTimestamp;
                    TItemDependencyReplica itemDependencyReplica;
                    
                    if(getBeforePutContext.HasClientUpdateSpaceValue())
                    {
                        const TDeserializedValueType& clientUpdateSpaceValue(getBeforePutContext.ClientUpdateSpaceValue());
                        itemDependencyTimestamp = clientUpdateSpaceValue.LogicalTimestamp();
                        itemDependencyReplica = clientUpdateSpaceValue.SourceReplicaId();
                    }
                    else
                    {
                        itemDependencyTimestamp = serializedValue.LogicalTimestamp;
                        itemDependencyReplica = serializedValue.SourceReplicaId;
                    }
                    
                    ConcludePut(getBeforePutContext.Key(),
                                getBeforePutContext.Value(),
                                getBeforePutContext.ClientId(),
                                getBeforePutContext.ClientDependencyTimestamp(),
                                getBeforePutContext.ClientDependencyPartition(),
                                getBeforePutContext.PutCompletedHandler(),
                                getBeforePutContext.Context(),
                                itemDependencyTimestamp,
                                itemDependencyReplica);
                }
                
                template<typename TCompletionHandler, typename TPutCompletedHandler, typename TContext>
                void HandlePrePutGetCompletionItemNotFound(TCompletionHandler& completedHandler,
                                                           const OrbeDelayedInstallPartitionServerGetBeforePutContext<TProtocolTraits, TPutCompletedHandler, TContext>& getBeforePutContext)
                {
                    TItemDependencyTimestamp itemDependencyTimestamp;
                    TItemDependencyReplica itemDependencyReplica;
                    
                    if(getBeforePutContext.HasClientUpdateSpaceValue())
                    {
                        const TDeserializedValueType& clientUpdateSpaceValue(getBeforePutContext.ClientUpdateSpaceValue());
                        itemDependencyTimestamp = clientUpdateSpaceValue.LogicalTimestamp();
                        itemDependencyReplica = clientUpdateSpaceValue.SourceReplicaId();
                    }
                    else
                    {
                        itemDependencyTimestamp = -1;
                        itemDependencyReplica = -1;
                    }
                    
                    ConcludePut(getBeforePutContext.Key(),
                                getBeforePutContext.Value(),
                                getBeforePutContext.ClientId(),
                                getBeforePutContext.ClientDependencyTimestamp(),
                                getBeforePutContext.ClientDependencyPartition(),
                                getBeforePutContext.PutCompletedHandler(),
                                getBeforePutContext.Context(),
                                itemDependencyTimestamp,
                                itemDependencyReplica);
                }
                
                void SendHeartbeatVector()
                {
                    m_replicaPartitionsBroadcaster.BroadcastHeartbeatVectorToPartitionReplicas(m_replicaId,
                                                                                               m_logicalVersionVector);
                }
                
                void ReceiveHeartbeatVector(const ReplicaIdType& senderReplicaId, const TVersionVector& versionVector)
                {
                    SR_ASSERT(senderReplicaId != ReplicaId());
                    
                    m_replicaVersionVectorSet[m_replicaId] = m_logicalVersionVector;
                    m_replicaVersionVectorSet[senderReplicaId] = versionVector;
                    
                    TVersionVector fullyReplicatedVersionVector(ComputeFullyReplicatedVersionVector());
                    
                    std::vector<TDeserializedValueType> updates;
                    
                    m_clientUpdateSpace.GetSafeUpdates(fullyReplicatedVersionVector, updates);
                    m_replicaUpdateSpace.GetSafeUpdates(fullyReplicatedVersionVector, updates);
                    
                    DumpUpdates(updates, "\n\nBEFORE:\n");
                    
                    sort(updates.begin(), updates.end(), [](const TDeserializedValueType& a, const TDeserializedValueType& b) {
                        return b.LogicalTimestamp() > a.LogicalTimestamp();
                    });
                    
                    DumpUpdates(updates, "\n\nAFTER:\n");
                    
                    m_replicationHandler.AddGlobalSpaceUpdates(updates);
                }
                
                void DumpStats()
                {
                    printf("Replica: %d, Partition: %d\n", m_replicaId, m_partitionId);
                    printf("\tNum Puts: %lu\n", m_numPuts);
                    
                    printf("\tStaging Vector: %s\n", Utilities::ToString(m_logicalVersionVector).c_str());
                    printf("\tGlobal Vector: %s\n", Utilities::ToString(m_globalLogicalVersionVector).c_str());
                    
                    TVersionVector fullyReplicatedVersionVector(ComputeFullyReplicatedVersionVector());
                    printf("\tFully Replicated Version Vector: %s\n", Utilities::ToString(fullyReplicatedVersionVector).c_str());
                    
                    printf("\tReplica Vector Set:\n");
                    for(auto r = 0; r < TProtocolTraits::NumReplicas; ++ r)
                    {
                        printf("\t\tReplica %d: %s\n", r, Utilities::ToString(m_replicaVersionVectorSet[r]).c_str());
                    }
                    
                    printf("\tNum Staging Replications Received: %lu\n", m_replicationHandler.NumStagingReplicationsReceived());
                    printf("\tNum Global Replications Received: %lu\n", m_replicationHandler.NumGlobalReplicationsReceived());
                    printf("\tNum Staging Replications Applied: %lu\n", m_replicationHandler.NumStagingReplicationsApplied());
                    printf("\tNum Global Replications Applied: %lu\n", m_replicationHandler.NumGlobalReplicationsApplied());
                    printf("\tNum Staging Replications Remaining Unapplied: %lu\n", m_replicationHandler.NumStagingReplicationsUnapplied());
                    printf("\tNum Global Replications Remaining Unapplied: %lu\n", m_replicationHandler.NumGlobalReplicationsUnapplied());
                    
                    for(auto p = 0; p < TProtocolTraits::NumPartitions; ++ p)
                    {
                        if(m_pendingTimestampQueries[p].IsPending())
                        {
                            printf("\tNum Pending Staging Replication Queries (Partition %d):"
                                   "\n\t\tStatus: Pending:"
                                   "\n\t\tPartition: %d"
                                   "\n\t\tSource Replica: %d"
                                   "\n\t\tDependency Timestamp: %d"
                                   "\n",
                                   p,
                                   m_pendingTimestampQueries[p].SenderPartitionId(),
                                   m_pendingTimestampQueries[p].SourceReplicaId(),
                                   m_pendingTimestampQueries[p].DependencyTimestamp());
                        }
                        else
                        {
                            printf("\tNum Pending Staging Replication Queries (Partition %d):\n\t\tStatus: Complete\n", p);
                        }
                    }
                    
                    printf("\tNum Pending Global Replication Queries: %lu\n", m_pendingGlobalTimestampQueries.size());
                }
                
            private:
                void TrySatisfyPendingTimestampQuery(TPendingLocalTimestampQuery& query)
                {
                    if(query.CanStatisfy(m_logicalVersionVector))
                    {
                        m_networkExchange.SendUnblockingTimestampDependencyCheckResponse(ReplicaId(),
                                                                                         query.SenderPartitionId(),
                                                                                         query.SourceReplicaId());
                        
                        query.Satisfy();
                        
                        m_pendingTimestampQueriesSatisfied += 1;
                    }
                }
                
                void TrySatisfyPendingTimestampQuery(TPendingGlobalTimestampQuery& query)
                {
                    if(query.CanStatisfy(m_globalLogicalVersionVector))
                    {
                        m_networkExchange.SendUnblockingGlobalTimestampDependencyCheckResponse(ReplicaId(),
                                                                                               query.SenderPartitionId(),
                                                                                               query.SourceReplicaId());
                        
                        query.Satisfy();
                        
                        m_pendingGlobalTimestampQueriesSatisfied += 1;
                    }
                }
                
                template<typename TGetCompletedHandler, typename TContext>
                void DispatchToGetCompleteItemFoundHandler(TGetCompletedHandler& getCompletedHandler,
                                                           TClientGetReplyValueType& reply,
                                                           TContext context)
                {
                    getCompletedHandler.HandleGetCompleteItemFound(reply, context);
                }
                
                
                template<typename TGetCompletedHandler>
                void DispatchToGetCompleteItemFoundHandler(TGetCompletedHandler& getCompletedHandler,
                                                           TClientGetReplyValueType& reply,
                                                           NoContext context)
                {
                    getCompletedHandler.HandleGetCompleteItemFound(reply);
                }
                
                template<typename TPutCompletedHandler, typename TContext>
                void DispatchToPutCompleteHandler(TPutCompletedHandler& putCompletedHandler,
                                                  TClientPutReplyValueType& reply,
                                                  TContext context)
                {
                    putCompletedHandler.HandlePutComplete(reply, context);
                }
                
                
                template<typename TPutCompletedHandler>
                void DispatchToPutCompleteHandler(TPutCompletedHandler& putCompletedHandler,
                                                  TClientPutReplyValueType& reply,
                                                  NoContext context)
                {
                    putCompletedHandler.HandlePutComplete(reply);
                }
                
                template<typename TPutCompletedHandler, typename TContext>
                void ConcludePut(const TStorageKey& key,
                                 const TClientInputValueType& input,
                                 const TClientId& clientId,
                                 const TClientDependencyTimestamp& clientDependencyTimestamp,
                                 const TClientDependencyPartition& clientDependencyPartition,
                                 TPutCompletedHandler& putCompletedHandler,
                                 const TContext& context,
                                 const TItemDependencyTimestamp& itemDependencyTimestamp,
                                 const TItemDependencyReplica& itemDependencyReplica)
                {
                    Utilities::TTimestamp timestamp = Utilities::Now();
                    
                    TLogicalTimestamp logicalTimestamp = m_logicalVersionVector.IncrementForReplica(m_replicaId);
                    
                    //printf("ConcludePut:: %s\n", ToString(m_logicalVersionVector).c_str());
                    
                    TDeserializedValueType value(key,
                                                 input,
                                                 logicalTimestamp,
                                                 itemDependencyTimestamp,
                                                 itemDependencyReplica,
                                                 clientId,
                                                 clientDependencyTimestamp,
                                                 clientDependencyPartition,
                                                 timestamp,
                                                 m_replicaId);
                    
                    TSerializedDataType serializedValue = m_serializer.Serialize(value);
                    
                    m_clientUpdateSpace.Put(clientId, key, value);
                    
                    m_storage.Put(key, serializedValue, *this, putCompletedHandler, value, context);
                }
                
                
                TVersionVector ComputeFullyReplicatedVersionVector() const
                {
                    TVersionVector fullyReplicatedVersionVector;
                    
                    for(auto r = 0; r < NumReplicas; ++ r)
                    {
                        auto minValue = GetMinimumValueForReplicaAcrossReplicatedVectorSet(r);
                        fullyReplicatedVersionVector.SetValueForReplica(r, minValue);
                    }
                    
                    return fullyReplicatedVersionVector;
                }
                
                TVersionVectorCounterType GetMinimumValueForReplicaAcrossReplicatedVectorSet(ReplicaIdType replicaToGetMinValueFor) const
                {
                    TVersionVectorCounterType minValue = TVersionVectorCounterType();
                    bool setValue = false;
                    
                    for(auto r = 0; r < NumReplicas; ++ r)
                    {
                        const TVersionVector& replicaVersionVector(m_replicaVersionVectorSet[r]);
                        
                        if(!setValue)
                        {
                            minValue = replicaVersionVector.GetValueForReplica(replicaToGetMinValueFor);
                            setValue = true;
                        }
                        
                        minValue = std::min(minValue, replicaVersionVector.GetValueForReplica(replicaToGetMinValueFor));
                    }
                    
                    SR_ASSERT(setValue);
                    return minValue;
                }
                
                void DumpUpdates(const std::vector<TDeserializedValueType>& updates, const char* message)
                {
#ifdef OrbeDelayedInstallPartitionServer_DEBUG_LOGGING
                    printf("%s", message);
                    
                    for(auto it = updates.begin(); it != updates.end(); ++ it)
                    {
                        const auto& value(*it);
                        printf("\tKey: %d, Value: %d, LogicalTimestamp: %d, SourceReplicaId: %d\n",
                               value.Key(), value.Value(), value.LogicalTimestamp(), value.SourceReplicaId());
                    }
#endif
                }
                
                TReplicationMessageType ReplicationMessageFromProtocolItem(const TDeserializedValueType& protocolItem,
                                                                           const PartitionIdType& partitionId)
                {
                    return TReplicationMessageType(protocolItem.Key(),
                                                   protocolItem.Value(),
                                                   protocolItem.LogicalTimestamp(),
                                                   protocolItem.ItemDependencyReplica(),
                                                   protocolItem.ItemDependencyTimestamp(),
                                                   protocolItem.ClientId(),
                                                   protocolItem.ClientDependencyTimestamp(),
                                                   protocolItem.ClientDependencyPartition(),
                                                   protocolItem.Timestamp(),
                                                   protocolItem.SourceReplicaId(),
                                                   partitionId);
                }
                
                ReplicaIdType m_replicaId;
                PartitionIdType m_partitionId;
                TStorage& m_storage;
                TNetworkExchange& m_networkExchange;
                TBroadcaster& m_replicaPartitionsBroadcaster;
                TSerializer& m_serializer;
                TClientUpdateSpace& m_clientUpdateSpace;
                TReplicaUpdateSpace& m_replicaUpdateSpace;
                TMetrics& m_metrics;
                TDelayTracking& m_delayTracking;
                TLogger& m_logger;
                TVersionVector m_logicalVersionVector;
                TVersionVector m_replicaVersionVectorSet[NumReplicas];
                TVersionVector m_globalLogicalVersionVector;
                OrbeDelayedInstallPartitionServerGetBeforePutHandler<TProtocolTraits> m_partitionServerGetBeforePutHandler;
                TReplicationHandler m_replicationHandler;
                
                size_t m_numPuts;
                
                //todo -- encapsulate these
                TPendingLocalTimestampQuery m_pendingTimestampQueries[NumPartitions];
                size_t m_pendingTimestampQueriesReceived;
                size_t m_pendingTimestampQueriesSatisfied;
                
                TPendingGlobalTimestampQueries m_pendingGlobalTimestampQueries;
                size_t m_pendingGlobalTimestampQueriesReceived;
                size_t m_pendingGlobalTimestampQueriesSatisfied;
            };
        }
    }
}
