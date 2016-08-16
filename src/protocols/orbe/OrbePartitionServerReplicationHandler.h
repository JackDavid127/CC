//
//  OrbePartitionServerReplicationHandler.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include <deque>
#include "Timing.h"
#include "Orbe.h"
#include "OrbeInFlightReplication.h"
#include "VersionVectorString.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            template<typename TProtocolTraits>
            class OrbePartitionServerReplicationHandler : private boost::noncopyable
            {
                typedef OrbeInFlightReplication<TProtocolTraits> TOrbeInFlightReplication;
                typedef std::deque<TOrbeInFlightReplication> TInFlightReplicationQueue;
                
                typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                typedef typename TProtocolTraits::TNetworkExchange TNetworkExchange;
                typedef typename TProtocolTraits::TDependencyMatrix TDependencyMatrix;
                typedef typename TProtocolTraits::TVersionVector TVersionVector;
                typedef typename TProtocolTraits::TSerializer TSerializer;
                typedef typename TProtocolTraits::TSerializedDataType TSerializedDataType;
                typedef typename TProtocolTraits::TDeserializedValueType TDeserializedValueType;
                typedef typename TProtocolTraits::TReplicationMessageType TReplicationMessageType;
                typedef typename TProtocolTraits::TStorageComponent TStorage;
                typedef typename TProtocolTraits::TMetrics TMetrics;
                typedef typename TProtocolTraits::TDelayTracking TDelayTracking;
                typedef typename TProtocolTraits::TLogger TLogger;
                
            public:
                OrbePartitionServerReplicationHandler(TLogger& logger,
                                                      TStorage& storage,
                                                      TSerializer& serializer,
                                                      TPartitionServer& partitionServer,
                                                      TNetworkExchange& networkExchange,
                                                      TDelayTracking& delayTracking)
                : m_logger(logger)
                , m_storage(storage)
                , m_serializer(serializer)
                , m_partitionServer(partitionServer)
                , m_networkExchange(networkExchange)
                , m_delayTracking(delayTracking)
                , m_replicationsReceived(0)
                , m_replicationsApplied(0)
                {
                }
                
                size_t NumReplicationsReceived() const
                {
                    return m_replicationsReceived;
                }
                
                size_t NumReplicationsApplied() const
                {
                    return m_replicationsApplied;
                }
                
                size_t NumReplicationsUnapplied() const
                {
                    size_t sum = 0;
                    
                    for(auto r = 0; r < TProtocolTraits::NumReplicas; ++ r)
                    {
                        sum += m_replicationQueues[r].size();
                    }
                    
                    return sum;
                }
                
                void ReceiveReplicateMessage(const TReplicationMessageType& replicateMessage)
                {
                    ++ m_replicationsReceived;
                    
                    ReplicaIdType replicaId = replicateMessage.SourceReplicaId();
                    
                    TInFlightReplicationQueue& replicaQueue = m_replicationQueues[replicaId];
                    
                    m_logger.Log("Received replicate message on P: %d, R: %d from R: %d, with timestamp %d, DM: %s, q size: %lu\n",
                                 m_partitionServer.PartitionId(),
                                 m_partitionServer.ReplicaId(),
                                 replicateMessage.SourceReplicaId(),
                                 replicateMessage.LogicalTimestamp(),
                                 replicateMessage.Matrix().ToString().c_str(),
                                 replicaQueue.size());
                    
                    TOrbeInFlightReplication replication(m_logger,
                                                         *this,
                                                         m_partitionServer,
                                                         replicateMessage);
                    
                    replicaQueue.push_back(replication);
                    
                    if(replicaQueue.size() == 1)
                    {
                        TryApplyReplication(false, replicaId);
                    }
                }
                
                void VerifyNeighbourPartitionVersionVectorGreaterOrEqualTo(const ReplicaIdType& sourceReplicaId,
                                                                           const PartitionIdType& neighbourPartitionId,
                                                                           const TVersionVector& vectorToVerify)
                {
                    //self.__vv_queries_made += 1
                    
                    const PartitionIdType myPartitionId(m_partitionServer.PartitionId());
                    const ReplicaIdType myReplicaId(m_partitionServer.ReplicaId());
                    
                    SR_ASSERT(neighbourPartitionId != myPartitionId);
                    SR_ASSERT(sourceReplicaId != myReplicaId);
                    
                    m_logger.Log("Requesting verify for %s at neighbour partition %d\n", ToString(vectorToVerify).c_str(), neighbourPartitionId);
                    
                    m_networkExchange.RequestIsNeighbourVersionVectorGreaterThanOrEqualToReplicationDependencyVector(myPartitionId,
                                                                                                                     myReplicaId,
                                                                                                                     neighbourPartitionId,
                                                                                                                     vectorToVerify,
                                                                                                                     sourceReplicaId);
                }
                
                void ReceiveNeighbourVersionVectorDependencyCheckResponse(ReplicaIdType sourceReplicaId)
                {
                    // self.__vv_queries_had_response += 1
                    TInFlightReplicationQueue& replicaQueue = m_replicationQueues[sourceReplicaId];
                    SR_ASSERT(replicaQueue.size() > 0);
                    
                    TOrbeInFlightReplication* pInFlightReplication = (&replicaQueue.front());
                    pInFlightReplication->VerifyNeighbouringPartitionCausality();
                    
                    if(!pInFlightReplication->BlockedOnNeighbouringPartitions())
                    {
                        TryApplyReplication(true, sourceReplicaId);
                    }
                }
                
                template<typename TContext = NoContext>
                void HandleGetCompleteItemFound(const TReplicationMessageType& replicateMessage,
                                                const TSerializedDataType& serializedValue,
                                                TContext context = EmptyContext)
                {
                    m_logger.Log("ReplicationHandler::HandleGetCompleteItemFound: %d, %d, %llu, %d\n",
                                 replicateMessage.Key(),
                                 replicateMessage.Value(),
                                 replicateMessage.Timestamp(),
                                 replicateMessage.SourceReplicaId());
                    
                    TDeserializedValueType existingItem = m_serializer.Deserialize(serializedValue);
                    
                    bool useNewItem = true;
                    
                    if(replicateMessage.ItemDependencyTimestamp() != existingItem.LogicalTimestamp())
                    {
                        if(existingItem.LogicalTimestamp() > replicateMessage.LogicalTimestamp())
                        {
                            useNewItem = false;
                        }
                        else
                        {
                            if(existingItem.LogicalTimestamp() == replicateMessage.LogicalTimestamp())
                            {
                                if(existingItem.SourceReplicaId() > replicateMessage.SourceReplicaId())
                                {
                                    useNewItem = false;
                                }
                            }
                        }
                    }
                    
                    if(useNewItem)
                    {
                        TDeserializedValueType newValue = replicateMessage.ToValue();
                        TSerializedDataType serializedNewValue = m_serializer.Serialize(newValue);
                        
                        MarkReplicationObserved(newValue, serializedNewValue);
                        
                        //self.__delay_tracking.operation_performed(new_ec_item)
                        
                        m_storage.Put(newValue.Key(),
                                      serializedNewValue,
                                      *this,
                                      replicateMessage,
                                      newValue);
                    }
                }
                
                template<typename TContext = NoContext>
                void HandleGetCompleteItemNotFound(const TReplicationMessageType& replicateMessage,
                                                   TContext context = EmptyContext)
                {
                    m_logger.Log("ReplicationHandler::HandleGetCompleteItemNotFound: %d, %d, %llu, %d\n",
                                 replicateMessage.Key(),
                                 replicateMessage.Value(),
                                 replicateMessage.Timestamp(),
                                 replicateMessage.SourceReplicaId());
                    
                    TDeserializedValueType protocolItem = replicateMessage.ToValue();
                    //self.__delay_tracking.operation_performed(new_ec_item)
                    TSerializedDataType serializedValue = m_serializer.Serialize(protocolItem);
                    
                    MarkReplicationObserved(protocolItem, serializedValue);
                    
                    m_storage.Put(protocolItem.Key(),
                                  serializedValue,
                                  *this,
                                  replicateMessage,
                                  protocolItem);
                }
                
                template<typename TContext = NoContext>
                void HandlePutComplete(const TReplicationMessageType& replicateMessage,
                                       const TDeserializedValueType& protocolItem,
                                       TContext context = EmptyContext)
                {
                    m_logger.Log("ReplicationHandler::HandlePutComplete: %d, %d, %llu, %d\n",
                                 replicateMessage.Key(),
                                 replicateMessage.Value(),
                                 replicateMessage.Timestamp(),
                                 replicateMessage.SourceReplicaId());
                }
                
            private:
                
                void MarkReplicationObserved(const TDeserializedValueType& protocolItem,
                                             TSerializedDataType& serializedValue) const
                {
                    // observe replication
                    SR_ASSERT(!protocolItem.ReplicationObserved());
                    auto observedTimestamp(Utilities::Now());
                    auto writeVisibilityLatency(Utilities::GetDeltaTime(observedTimestamp, protocolItem.Timestamp()));
                    m_delayTracking.OperationPerformed(writeVisibilityLatency);
                    serializedValue.ObservedTimestamp = observedTimestamp;
                }
                
                void TryApplyReplication(bool fromNeighbourDependencySatisfied,
                                         ReplicaIdType replicaId)
                {
                    TInFlightReplicationQueue& replicaQueue = m_replicationQueues[replicaId];
                    SR_ASSERT(replicaQueue.size() > 0);
                    
                    TOrbeInFlightReplication* pInFlightReplication = (&replicaQueue.front());
                    
                    if(fromNeighbourDependencySatisfied)
                    {
                        SR_ASSERT(pInFlightReplication->IsActive());
                    }
                    else
                    {
                        SR_ASSERT(!pInFlightReplication->IsActive());
                    }
                    
                    bool shouldCheckActive = !fromNeighbourDependencySatisfied;
                    
                    while(pInFlightReplication != nullptr)
                    {
                        if(shouldCheckActive)
                        {
                            pInFlightReplication->Activate();
                        }
                        shouldCheckActive = true;
                        
                        const TVersionVector& logicalVersionVector = m_partitionServer.LogicalVersionVector();
                        SR_ASSERT(pInFlightReplication->DependenciesAreInstalled(logicalVersionVector, m_partitionServer.PartitionId()));
                        
                        if(pInFlightReplication->BlockedOnNeighbouringPartitions())
                        {
                            pInFlightReplication->DispatchNeighbourPartitionDependencyChecks();
                            break;
                        }
                        
                        ApplyReplicationUpdate(replicaId);
                        
                        pInFlightReplication = (replicaQueue.size() > 0) ? (&replicaQueue.front()) : nullptr;
                    }
                }
                
                void ApplyReplicationUpdate(ReplicaIdType replicaId)
                {
                    TInFlightReplicationQueue& replicaQueue = m_replicationQueues[replicaId];
                    SR_ASSERT(replicaQueue.size() > 0);
                    TOrbeInFlightReplication& inFlightReplication = replicaQueue.front();
                    
                    /*m_logger.Log("Apply replicate message on P: %d, R: %d from R: %d, with timestamp %d, q size: %d\n",
                     m_partitionServer.PartitionId(),
                     m_partitionServer.ReplicaId(),
                     replicaId,
                     inFlightReplication.LogicalTimestamp(),
                     replicaQueue.size());*/
                    
                    m_partitionServer.IncrementLogicalVersionVectorFromReplication(replicaId,
                                                                                   inFlightReplication.LogicalTimestamp());
                    
                    m_partitionServer.TrySatisfyPendingVersionVectorQueries();
                    
                    const TReplicationMessageType& replicateMessage = inFlightReplication.ReplicateMessage();
                    m_storage.Get(replicateMessage.Key(), *this, replicateMessage, EmptyContext);
                    
                    ++ m_replicationsApplied;
                    
                    replicaQueue.pop_front();
                }
                
                TLogger& m_logger;
                TStorage& m_storage;
                TSerializer& m_serializer;
                TInFlightReplicationQueue m_replicationQueues[TProtocolTraits::NumReplicas];
                TPartitionServer& m_partitionServer;
                TNetworkExchange& m_networkExchange;
                TDelayTracking& m_delayTracking;
                size_t m_replicationsReceived;
                size_t m_replicationsApplied;
            };
        }
    }
}
