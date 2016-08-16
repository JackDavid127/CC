//
//  OrbePartitionServer.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include "Timing.h"
#include "Orbe.h"
#include "OrbePartitionServerGetBeforePutHandler.h"
#include "OrbePartitionServerGetBeforePutContext.h"
#include "OrbePendingVersionVectorQuery.h"
#include "VersionVectorString.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            template<typename TProtocolTraits>
            class OrbePartitionServer : private boost::noncopyable
            {
                typedef OrbePartitionServer<TProtocolTraits> TSelf;
                static const size_t NumPartitions = TProtocolTraits::NumPartitions;
                static const size_t NumReplicas = TProtocolTraits::NumReplicas;
                
                typedef typename TProtocolTraits::TVersionVector TVersionVector;
                typedef typename TProtocolTraits::TDependencyMatrix TDependencyMatrix;
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
                
                typedef OrbePendingVersionVectorQuery<TProtocolTraits> TPendingVersionVectorQuery;
                typedef std::vector<TPendingVersionVectorQuery> TPendingVersionVectorQueries;
                
            public:
                OrbePartitionServer(ReplicaIdType replicaId,
                                    PartitionIdType partitionId,
                                    TStorage& storage,
                                    TNetworkExchange& networkExchange,
                                    TBroadcaster& replicaPartitionsBroadcaster,
                                    TSerializer& serializer,
                                    TMetrics& metrics,
                                    TDelayTracking& delayTracking,
                                    TLogger& logger)
                : m_replicaId(replicaId)
                , m_partitionId(partitionId)
                , m_storage(storage)
                , m_networkExchange(networkExchange)
                , m_replicaPartitionsBroadcaster(replicaPartitionsBroadcaster)
                , m_serializer(serializer)
                , m_metrics(metrics)
                , m_delayTracking(delayTracking)
                , m_logger(logger)
                , m_logicalVersionVector()
                , m_partitionServerGetBeforePutHandler(*this)
                , m_replicationHandler(logger, storage, serializer, *this, networkExchange, delayTracking)
                , m_numPuts(0)
                , m_versionVectorQueries()
                , m_versionVectorQueriesReceived(0)
                , m_versionVectorQueriesSatisfied(0)
                {
                    
                }
                
                ~OrbePartitionServer()
                {
                    
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
                
                size_t TotalUnappliedReplications() const
                {
                    return 0;
                }
                
                template<typename TGetCompletedHandler, typename TContext = NoContext>
                void Get(const TStorageKey& key,
                         TGetCompletedHandler& getCompletedHandler,
                         TContext context = EmptyContext)
                {
                    //self.__num_gets.increment_and_get();
                    m_storage.Get(key, *this, getCompletedHandler, context);
                }
                
                template<typename TPutCompletedHandler, typename TContext = NoContext>
                void Put(const TStorageKey& key,
                         const TClientInputValueType& input,
                         TPutCompletedHandler& putCompletedHandler,
                         const TDependencyMatrix& dependencyMatrix,
                         TContext context = EmptyContext)
                {
                    ++ m_numPuts;
                    
                    //self.__num_puts.increment_and_get()
                    
                    //m_logger.Log("partition server replica %d, partition %d\n", m_replicaId, m_partitionId);
                    
                    typedef OrbePartitionServerGetBeforePutContext<TProtocolTraits, TPutCompletedHandler, TContext> TGetBeforePutContext;
                    
                    auto additionalContext = TGetBeforePutContext(key, dependencyMatrix, input, putCompletedHandler, context);
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
                    TReplicationMessageType message(protocolItem.Key(),
                                                    protocolItem.Value(),
                                                    protocolItem.LogicalTimestamp(),
                                                    protocolItem.ItemDependencyTimestamp(),
                                                    protocolItem.DependencyMatrix(),
                                                    protocolItem.Timestamp(),
                                                    protocolItem.SourceReplicaId(),
                                                    m_partitionId);
                    
                    m_replicaPartitionsBroadcaster.BroadcastReplicationToPartitionReplicas(message);
                    
                    TClientPutReplyValueType reply(protocolItem.Timestamp(),
                                                   protocolItem.SourceReplicaId(),
                                                   protocolItem.LogicalTimestamp());
                    
                    DispatchToPutCompleteHandler(putCompletedHandler, reply, context);
                }
                
                void ReceiveReplicateMessage(const TReplicationMessageType& replicateMessage)
                {
                    m_logger.Log("OrbePartitionServer::ReceiveReplicateMessage: Key: %d, LogicalTimestamp: %llu, SourceReplicaId:%d\n",
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
                
                void ReceiveNeighbourVersionVectorDependencyCheckRequest(PartitionIdType senderPartitionId,
                                                                         const TVersionVector& dependencyVersionVector,
                                                                         ReplicaIdType sourceReplicaId)
                {
                    m_versionVectorQueriesReceived += 1;
                    
                    m_logger.Log("Receive verify request from partition %d for vector: %s\n",
                                 senderPartitionId,
                                 ToString(dependencyVersionVector).c_str());
                    
                    // one of our neighbouring partition servers within this replication site wants to know if our version vector satisfies
                    // one of its in-flight replication dependencies...
                    //
                    // we will call it back when this is the case
                    TPendingVersionVectorQuery query;
                    
                    query.SetPendingQuery(dependencyVersionVector,
                                          senderPartitionId,
                                          sourceReplicaId);
                    
                    TrySatisfyVersionVectorQuery(query);
                    
                    if(query.IsPending())
                    {
                        // not satisfied
                        m_versionVectorQueries.push_back(query);
                        SR_ASSERT(m_versionVectorQueries.size() <= (NumPartitions * NumReplicas));
                    }
                }
                
                void ReceiveNeighbourVersionVectorDependencyCheckResponse(ReplicaIdType sourceReplicaId)
                {
                    m_replicationHandler.ReceiveNeighbourVersionVectorDependencyCheckResponse(sourceReplicaId);
                }
                
                void IncrementLogicalVersionVectorFromReplication(ReplicaIdType sourceReplicaId,
                                                                  TLogicalTimestamp expectedValue)
                {
                    SR_ASSERT((m_logicalVersionVector.GetValueForReplica(sourceReplicaId) + 1) == (expectedValue));
                    m_logicalVersionVector.IncrementForReplica(sourceReplicaId);
                    //printf("IncrementLogicalVersionVectorFromReplication:: %s\n", ToString(m_logicalVersionVector).c_str());
                }
                
                void TrySatisfyPendingVersionVectorQueries()
                {
                    const auto valueCount(m_versionVectorQueries.size());
                    SR_ASSERT(valueCount <= (NumPartitions * NumReplicas));
                    
                    for(auto i = 0; i < valueCount; ++ i)
                    {
                        const auto valueIndex = (valueCount - 1) - i;
                        auto& query(m_versionVectorQueries[valueIndex]);
                        
                        SR_ASSERT(query.IsPending());
                        
                        TrySatisfyVersionVectorQuery(query);
                        
                        if(!query.IsPending())
                        {
                            std::swap(query, m_versionVectorQueries.back());
                            m_versionVectorQueries.pop_back();
                        }
                    }
                }
                
                
                template<typename TCompletionHandler, typename TPutCompletedHandler, typename TContext>
                void HandlePrePutGetCompletionItemFound(TCompletionHandler& completedHandler,
                                                        const TSerializedDataType& serializedValue,
                                                        const OrbePartitionServerGetBeforePutContext<TProtocolTraits, TPutCompletedHandler, TContext>& getBeforePutContext)
                {
                    ConcludePut(getBeforePutContext.Key(),
                                getBeforePutContext.Value(),
                                getBeforePutContext.Matrix(),
                                getBeforePutContext.PutCompletedHandler(),
                                getBeforePutContext.Context(),
                                serializedValue.LogicalTimestamp);
                }
                
                template<typename TCompletionHandler, typename TPutCompletedHandler, typename TContext>
                void HandlePrePutGetCompletionItemNotFound(TCompletionHandler& completedHandler,
                                                           const OrbePartitionServerGetBeforePutContext<TProtocolTraits, TPutCompletedHandler, TContext>& getBeforePutContext)
                {
                    ConcludePut(getBeforePutContext.Key(),
                                getBeforePutContext.Value(),
                                getBeforePutContext.Matrix(),
                                getBeforePutContext.PutCompletedHandler(),
                                getBeforePutContext.Context(),
                                -1);
                }
                
                
                void DumpStats()
                {
                    printf("Replica: %d, Partition: %d\n", m_replicaId, m_partitionId);
                    printf("\tNum Puts: %lu\n", m_numPuts);
                    
                    printf("\tLogical Vector: %s\n", Utilities::ToString(m_logicalVersionVector).c_str());
                    
                    printf("\tNum Replications Received: %lu\n", m_replicationHandler.NumReplicationsReceived());
                    printf("\tNum Replications Applied: %lu\n", m_replicationHandler.NumReplicationsApplied());
                    printf("\tNum Replications Remaining Unapplied: %lu\n", m_replicationHandler.NumReplicationsUnapplied());
                    
                    printf("\tNum Version Vector Queries Received: %lu\n", m_versionVectorQueriesReceived);
                    printf("\tNum Version Vector Queries Satisfied: %lu\n", m_versionVectorQueriesSatisfied);
                    
                    for(auto p = 0; p < m_versionVectorQueries.size(); ++ p)
                    {
                        if(m_versionVectorQueries[p].IsPending())
                        {
                            printf("\tNum Pending Replication Queries (query %d):"
                                   "\n\t\tStatus: Pending:"
                                   "\n\t\tPartition: %d"
                                   "\n\t\tSource Replica: %d"
                                   "\n\t\tVector: %s"
                                   "\n",
                                   p,
                                   m_versionVectorQueries[p].SenderPartitionId(),
                                   m_versionVectorQueries[p].SourceReplicaId(),
                                   Utilities::ToString(m_versionVectorQueries[p].Vector()).c_str());
                        }
                        else
                        {
                            printf("\tNum Pending Staging Replication Queries (Partition %d):\n\t\tStatus: Complete\n", p);
                        }
                    }
                }
                
                
            private:
                void TrySatisfyVersionVectorQuery(TPendingVersionVectorQuery& query)
                {
                    if(query.CanStatisfy(m_logicalVersionVector))
                    {
                        m_networkExchange.SendUnblockingVersionVectorDependencyCheckResponse(ReplicaId(),
                                                                                             query.SenderPartitionId(),
                                                                                             query.SourceReplicaId());
                        
                        query.Satisfy();
                        
                        m_versionVectorQueriesSatisfied += 1;
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
                                 const TDependencyMatrix& dependencyMatrix,
                                 TPutCompletedHandler& putCompletedHandler,
                                 const TContext& context,
                                 const TLogicalTimestamp& itemDependencyTimestamp)
                {
                    Utilities::TTimestamp timestamp = Utilities::Now();
                    
                    TLogicalTimestamp logicalTimestamp = m_logicalVersionVector.IncrementForReplica(m_replicaId);
                    
                    //printf("ConcludePut:: %s\n", ToString(m_logicalVersionVector).c_str());
                    
                    TDeserializedValueType value(key,
                                                 input,
                                                 logicalTimestamp,
                                                 itemDependencyTimestamp,
                                                 dependencyMatrix,
                                                 timestamp,
                                                 m_replicaId);
                    
                    TSerializedDataType serializedValue = m_serializer.Serialize(value);
                    m_storage.Put(key, serializedValue, *this, putCompletedHandler, value, context);
                }
                
                ReplicaIdType m_replicaId;
                PartitionIdType m_partitionId;
                TStorage& m_storage;
                TNetworkExchange& m_networkExchange;
                TBroadcaster& m_replicaPartitionsBroadcaster;
                TSerializer& m_serializer;
                TMetrics& m_metrics;
                TDelayTracking& m_delayTracking;
                TLogger& m_logger;
                TVersionVector m_logicalVersionVector;
                OrbePartitionServerGetBeforePutHandler<TProtocolTraits> m_partitionServerGetBeforePutHandler;
                TReplicationHandler m_replicationHandler;
                size_t m_numPuts;
                TPendingVersionVectorQueries m_versionVectorQueries;
                size_t m_versionVectorQueriesReceived;
                size_t m_versionVectorQueriesSatisfied;
            };
        }
    }
}
