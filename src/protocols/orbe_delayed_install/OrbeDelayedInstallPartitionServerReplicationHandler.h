//
//  OrbeDelayedInstallPartitionServerReplicationHandler.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include <deque>
#include "Timing.h"
#include "OrbeDelayedInstall.h"
#include "OrbeDelayedInstallInFlightReplication.h"
#include "VersionVectorString.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            template<typename TProtocolTraits>
            class OrbeDelayedInstallPartitionServerReplicationHandler : private boost::noncopyable
            {
                typedef OrbeDelayedInstallInFlightReplication<TProtocolTraits> TOrbeDelayedInstallInFlightReplication;
                typedef std::deque<TOrbeDelayedInstallInFlightReplication> TInFlightReplicationQueue;
                
                typedef OrbeDelayedInstallInFlightApplicationToGlobalSpace<TProtocolTraits> TOrbeDelayedInstallGlobalSpaceInFlightReplication;
                typedef std::deque<TOrbeDelayedInstallGlobalSpaceInFlightReplication> TInFlightGlobalSpaceReplicationQueue;
                
                typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                typedef typename TProtocolTraits::TNetworkExchange TNetworkExchange;
                typedef typename TProtocolTraits::TClientDependencyTimestamp TClientDependencyTimestamp;
                typedef typename TProtocolTraits::TClientDependencyPartition TClientDependencyPartition;
                typedef typename TProtocolTraits::TVersionVector TVersionVector;
                typedef typename TProtocolTraits::TSerializer TSerializer;
                typedef typename TProtocolTraits::TSerializedDataType TSerializedDataType;
                typedef typename TProtocolTraits::TDeserializedValueType TDeserializedValueType;
                typedef typename TProtocolTraits::TReplicationMessageType TReplicationMessageType;
                typedef typename TProtocolTraits::TStorageComponent TStorage;
                typedef typename TProtocolTraits::TMetrics TMetrics;
                typedef typename TProtocolTraits::TDelayTracking TDelayTracking;
                typedef typename TProtocolTraits::TReplicaUpdateSpace TReplicaUpdateSpace;
                typedef typename TProtocolTraits::TLogger TLogger;
                
            public:
                OrbeDelayedInstallPartitionServerReplicationHandler(TLogger& logger,
                                                                    TStorage& storage,
                                                                    TSerializer& serializer,
                                                                    TPartitionServer& partitionServer,
                                                                    TNetworkExchange& networkExchange,
                                                                    TReplicaUpdateSpace& replicaUpdateSpace,
                                                                    TDelayTracking& delayTracking)
                : m_logger(logger)
                , m_storage(storage)
                , m_serializer(serializer)
                , m_partitionServer(partitionServer)
                , m_networkExchange(networkExchange)
                , m_replicaUpdateSpace(replicaUpdateSpace)
                , m_delayTracking(delayTracking)
                , m_replicationsReceivedToLocalSpace(0)
                , m_replicationsReceivedToGlobalSpace(0)
                , m_replicationsAppliedToLocalSpace(0)
                , m_replicationsAppliedToGlobalSpace(0)
                {
                }
                
                size_t NumStagingReplicationsReceived() const
                {
                    return m_replicationsReceivedToLocalSpace;
                }
                
                size_t NumGlobalReplicationsReceived() const
                {
                    return m_replicationsReceivedToGlobalSpace;
                }
                
                size_t NumStagingReplicationsApplied() const
                {
                    return m_replicationsAppliedToLocalSpace;
                }
                
                size_t NumGlobalReplicationsApplied() const
                {
                    return m_replicationsAppliedToGlobalSpace;
                }
                
                size_t NumStagingReplicationsUnapplied() const
                {
                    size_t sum = 0;
                    
                    for(auto r = 0; r < TProtocolTraits::NumReplicas; ++ r)
                    {
                        sum += m_replicationQueues[r].size();
                    }
                    
                    return sum;
                }
                
                size_t NumGlobalReplicationsUnapplied() const
                {
                    size_t sum = 0;
                    
                    for(auto r = 0; r < TProtocolTraits::NumReplicas; ++ r)
                    {
                        sum += m_globalReplicationQueues[r].size();
                    }
                    
                    return sum;
                }
                
                void ReceiveReplicateMessage(const TReplicationMessageType& replicateMessage)
                {
                    ++ m_replicationsReceivedToLocalSpace;
                    
                    ReplicaIdType replicaId = replicateMessage.SourceReplicaId();
                    
                    TInFlightReplicationQueue& replicaQueue = m_replicationQueues[replicaId];
                    
                    m_logger.Log("Received replicate message on P: %d, R: %d from R: %d, with timestamp %d, CDP: %d, CDT: %d, q size: %lu\n",
                                 m_partitionServer.PartitionId(),
                                 m_partitionServer.ReplicaId(),
                                 replicateMessage.SourceReplicaId(),
                                 replicateMessage.LogicalTimestamp(),
                                 replicateMessage.ClientDependencyPartition(),
                                 replicateMessage.ClientDependencyTimestamp(),
                                 replicaQueue.size());
                    
                    TOrbeDelayedInstallInFlightReplication replication(m_logger,
                                                                       *this,
                                                                       m_partitionServer,
                                                                       replicateMessage.ToValue());
                    
                    replicaQueue.push_back(replication);
                    
                    if(replicaQueue.size() == 1)
                    {
                        TryApplyReplication(false, replicaId);
                    }
                }
                
                void VerifyNeighbourPartitionTimestampGreaterOrEqualTo(const TClientDependencyPartition& dependencyPartitionId,
                                                                       const ReplicaIdType& sourceReplicaId,
                                                                       const TClientDependencyTimestamp& dependencyTimestamp)
                {
                    //self.__vv_queries_made += 1
                    
                    const PartitionIdType neighbourPartitionId(dependencyPartitionId);
                    const PartitionIdType myPartitionId(m_partitionServer.PartitionId());
                    const ReplicaIdType myReplicaId(m_partitionServer.ReplicaId());
                    
                    SR_ASSERT(neighbourPartitionId != myPartitionId);
                    SR_ASSERT(sourceReplicaId != myReplicaId);
                    
                    //printf("Verifying %s\n", ToString(vectorToVerify).c_str());
                    
                    m_networkExchange.RequestIsNeighbourTimestampGreaterThanOrEqualToReplicationDependencyTimestamp(myPartitionId,
                                                                                                                    myReplicaId,
                                                                                                                    neighbourPartitionId,
                                                                                                                    dependencyTimestamp,
                                                                                                                    sourceReplicaId);
                }
                
                void ReceiveNeighbourTimestampDependencyCheckResponse(ReplicaIdType sourceReplicaId)
                {
                    // self.__vv_queries_had_response += 1
                    TInFlightReplicationQueue& replicaQueue = m_replicationQueues[sourceReplicaId];
                    SR_ASSERT(!replicaQueue.empty());
                    
                    TOrbeDelayedInstallInFlightReplication* pInFlightReplication = (&replicaQueue.front());
                    
                    // query must be active
                    SR_ASSERT(pInFlightReplication->IsActive());
                    
                    // verify query
                    pInFlightReplication->VerifyNeighbouringPartitionCausality();
                    
                    // Orbe+ should only depend on exactly one neighbour
                    SR_ASSERT(!pInFlightReplication->BlockedOnNeighbouringPartitions());
                    SR_ASSERT(pInFlightReplication->HasInstalledDependencies());
                    
                    // apply it
                    ApplyLocalSpaceReplicationUpdate(sourceReplicaId);
                    
                    if(!replicaQueue.empty())
                    {
                        // we should try to apply the next, as this will loop for the next replication
                        TryApplyReplication(false, sourceReplicaId);
                    }
                }
                
                void VerifyNeighbourPartitionGlobalTimestampGreaterOrEqualTo(const TClientDependencyPartition& dependencyPartitionId,
                                                                             const ReplicaIdType& sourceReplicaId,
                                                                             const TClientDependencyTimestamp& dependencyTimestamp)
                {
                    //self.__vv_queries_made += 1
                    
                    const PartitionIdType neighbourPartitionId(dependencyPartitionId);
                    const PartitionIdType myPartitionId(m_partitionServer.PartitionId());
                    const ReplicaIdType myReplicaId(m_partitionServer.ReplicaId());
                    
                    SR_ASSERT(neighbourPartitionId != myPartitionId);
                    
                    //printf("Verifying %s\n", ToString(vectorToVerify).c_str());
                    
                    m_networkExchange.RequestIsNeighbourGlobalTimestampNotLessThanDependencyGlobalTimestamp(myPartitionId,
                                                                                                            myReplicaId,
                                                                                                            neighbourPartitionId,
                                                                                                            dependencyTimestamp,
                                                                                                            sourceReplicaId);
                }
                
                void ReceiveNeighbourGlobalTimestampDependencyCheckResponse(ReplicaIdType sourceReplicaId)
                {
                    // self.__vv_queries_had_response += 1
                    TInFlightGlobalSpaceReplicationQueue& replicaQueue = m_globalReplicationQueues[sourceReplicaId];
                    SR_ASSERT(!replicaQueue.empty());
                    
                    TOrbeDelayedInstallInFlightReplication* pInFlightReplication = (&replicaQueue.front());
                    
                    // query must be active
                    SR_ASSERT(pInFlightReplication->IsActive());
                    
                    // verify query
                    pInFlightReplication->VerifyNeighbouringPartitionCausality();
                    
                    // Orbe+ should only depend on exactly one neighbour
                    SR_ASSERT(!pInFlightReplication->BlockedOnNeighbouringPartitions());
                    SR_ASSERT(pInFlightReplication->HasInstalledDependencies());
                    
                    // apply it
                    ApplyGlobalSpaceReplication(sourceReplicaId);
                    
                    if(!replicaQueue.empty())
                    {
                        // we should '__try_apply', as this will loop for the next replication
                        TryApplyNextGlobalSpaceReplication(sourceReplicaId);
                    }
                }
                
                void AddGlobalSpaceUpdates(const std::vector<TDeserializedValueType>& globalSpaceUpdates)
                {
                    for(auto it = globalSpaceUpdates.begin(); it != globalSpaceUpdates.end(); ++ it)
                    {
                        const TDeserializedValueType& protocolItem(*it);
                        
                        TOrbeDelayedInstallGlobalSpaceInFlightReplication globalSpaceApplication(m_logger,
                                                                                                 *this,
                                                                                                 m_partitionServer,
                                                                                                 protocolItem);
                        
                        TInFlightGlobalSpaceReplicationQueue& queue(m_globalReplicationQueues[protocolItem.SourceReplicaId()]);
                        queue.push_back(globalSpaceApplication);
                        
                        ++ m_replicationsReceivedToGlobalSpace;
                    }
                    
                    for(auto r = 0; r < TProtocolTraits::NumReplicas; ++ r)
                    {
                        TInFlightGlobalSpaceReplicationQueue& queue = m_globalReplicationQueues[r];
                        
                        if(!queue.empty() && !queue.front().IsActive())
                        {
                            TryApplyNextGlobalSpaceReplication(r);
                        }
                    }
                }
                
                template<typename TContext = NoContext>
                void HandleGetCompleteItemFound(const TDeserializedValueType& protocolItem,
                                                const TSerializedDataType& serializedValue,
                                                TContext context = EmptyContext)
                {
                    m_logger.Log("ReplicationHandler::HandleGetCompleteItemFound: %d, %d, %llu, %d\n",
                                 protocolItem.Key(),
                                 protocolItem.Value(),
                                 protocolItem.Timestamp(),
                                 protocolItem.SourceReplicaId());
                    
                    TDeserializedValueType existingItem = m_serializer.Deserialize(serializedValue);
                    
                    bool useNewItem = true;
                    
                    if(protocolItem.SourceReplicaId() == existingItem.SourceReplicaId())
                    {
                        //everything is cool, we apply the new one
                    }
                    else if(protocolItem.ItemDependencyTimestamp() == existingItem.LogicalTimestamp()
                            && protocolItem.ItemDependencyReplica() == existingItem.SourceReplicaId())
                    {
                        //everything is cool, we apply the new one
                    }
                    else
                    {
                        if(existingItem.LogicalTimestamp() > protocolItem.LogicalTimestamp())
                        {
                            useNewItem = false;
                        }
                        else
                        {
                            if(existingItem.LogicalTimestamp() == protocolItem.LogicalTimestamp())
                            {
                                if(existingItem.SourceReplicaId() > protocolItem.SourceReplicaId())
                                {
                                    useNewItem = false;
                                }
                            }
                        }
                    }
                    
                    if(useNewItem)
                    {
                        TSerializedDataType serializedNewValue = m_serializer.Serialize(protocolItem);
                        
                        MarkReplicationObserved(protocolItem, serializedNewValue);
                        
                        //self.__delay_tracking.operation_performed(new_ec_item)
                        
                        m_storage.Put(protocolItem.Key(),
                                      serializedNewValue,
                                      *this,
                                      protocolItem,
                                      protocolItem);
                    }
                }
                
                template<typename TContext = NoContext>
                void HandleGetCompleteItemNotFound(const TDeserializedValueType& protocolItem,
                                                   TContext context = EmptyContext)
                {
                    m_logger.Log("ReplicationHandler::HandleGetCompleteItemNotFound: %d, %d, %llu, %d\n",
                                 protocolItem.Key(),
                                 protocolItem.Value(),
                                 protocolItem.Timestamp(),
                                 protocolItem.SourceReplicaId());
                    
                    //self.__delay_tracking.operation_performed(new_ec_item)
                    TSerializedDataType serializedValue = m_serializer.Serialize(protocolItem);
                    
                    MarkReplicationObserved(protocolItem, serializedValue);
                    
                    m_storage.Put(protocolItem.Key(),
                                  serializedValue,
                                  *this,
                                  protocolItem,
                                  protocolItem);
                }
                
                template<typename TContext = NoContext>
                void HandlePutComplete(const TDeserializedValueType& replicateMessage,
                                       const TDeserializedValueType& protocolItem,
                                       TContext context = EmptyContext)
                {
                    m_logger.Log("ReplicationHandler::HandlePutComplete: %d, %d, %llu, %d\n",
                                 protocolItem.Key(),
                                 protocolItem.Value(),
                                 protocolItem.Timestamp(),
                                 protocolItem.SourceReplicaId());
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
                    SR_ASSERT(!replicaQueue.empty());
                    
                    TOrbeDelayedInstallInFlightReplication* pInFlightReplication = (&replicaQueue.front());
                    
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
                        const auto dependenciesAreInstalled(pInFlightReplication->DependenciesAreInstalled(logicalVersionVector));
                        SR_ASSERT(dependenciesAreInstalled);
                        
                        if(pInFlightReplication->BlockedOnNeighbouringPartitions())
                        {
                            pInFlightReplication->DispatchNeighbourPartitionDependencyChecks();
                            break;
                        }
                        
                        ApplyLocalSpaceReplicationUpdate(replicaId);
                        
                        pInFlightReplication = (!replicaQueue.empty()) ? (&replicaQueue.front()) : nullptr;
                    }
                }
                
                void ApplyLocalSpaceReplicationUpdate(ReplicaIdType replicaId)
                {
                    TInFlightReplicationQueue& replicaQueue = m_replicationQueues[replicaId];
                    SR_ASSERT(replicaQueue.size() > 0);
                    TOrbeDelayedInstallInFlightReplication& inFlightReplication = replicaQueue.front();
                    
                    /*m_logger.Log("Apply replicate message on P: %d, R: %d from R: %d, with timestamp %d, q size: %d\n",
                     m_partitionServer.PartitionId(),
                     m_partitionServer.ReplicaId(),
                     replicaId,
                     inFlightReplication.LogicalTimestamp(),
                     replicaQueue.size());*/
                    
                    m_partitionServer.IncrementLogicalVersionVectorFromReplication(replicaId,
                                                                                   inFlightReplication.LogicalTimestamp());
                    
                    //
                    const TDeserializedValueType& value = inFlightReplication.Value();
                    m_replicaUpdateSpace.Put(value.Key(), value);
                    
                    replicaQueue.pop_front();
                    
                    ++ m_replicationsAppliedToLocalSpace;
                    
                    m_partitionServer.TrySatisfyPendingLocalTimestampQueries();
                }
                
                void ApplyGlobalSpaceReplication(ReplicaIdType replicaId)
                {
                    TInFlightGlobalSpaceReplicationQueue& replicaQueue = m_globalReplicationQueues[replicaId];
                    SR_ASSERT(!replicaQueue.empty());
                    TOrbeDelayedInstallGlobalSpaceInFlightReplication& inFlightReplication = replicaQueue.front();
                    
                    m_partitionServer.IncrementGlobalLogicalVersionVector(replicaId,
                                                                          inFlightReplication.LogicalTimestamp());
                    
                    m_partitionServer.TrySatisfyPendingGlobalTimestampQueries();
                    
                    for(auto r = 0; r < TProtocolTraits::NumReplicas; ++ r)
                    {
                        TInFlightGlobalSpaceReplicationQueue& queue = m_globalReplicationQueues[r];
                        if(!queue.empty())
                        {
                            TOrbeDelayedInstallGlobalSpaceInFlightReplication& head = queue.front();
                            if(head.IsActive() && head.BlockedOnLocalDependency())
                            {
                                if(head.DependenciesAreInstalled(m_partitionServer.LogicalGlobalVersionVector()))
                                {
                                    head.Unblock();
                                    
                                    if(head.BlockedOnNeighbouringPartitions())
                                    {
                                        head.DispatchGlobalNeighbourPartitionDependencyChecks();
                                    }
                                    else
                                    {
                                        ApplyGlobalSpaceReplication(r);
                                        TryApplyNextGlobalSpaceReplication(r);
                                    }
                                }
                            }
                        }
                        
                    }
                    
                    ++ m_replicationsAppliedToGlobalSpace;
                    
                    const TDeserializedValueType& value = inFlightReplication.Value();
                    m_storage.Get(value.Key(), *this, value, EmptyContext);
                    
                    replicaQueue.pop_front();
                }
                
                void TryApplyNextGlobalSpaceReplication(ReplicaIdType sourceReplicaId)
                {
                    TInFlightGlobalSpaceReplicationQueue& replicaQueue = m_globalReplicationQueues[sourceReplicaId];
                    SR_ASSERT(replicaQueue.empty() || !replicaQueue.front().IsActive());
                    
                    // apply them until we run out or we become blocked on a neighbour dependency
                    
                    while(!replicaQueue.empty())
                    {
                        TOrbeDelayedInstallGlobalSpaceInFlightReplication& inFlightReplication = replicaQueue.front();
                        
                        inFlightReplication.Activate();
                        
                        const TVersionVector& logicalVersionVector = m_partitionServer.LogicalGlobalVersionVector();
                        const auto dependenciesAreInstalled(inFlightReplication.DependenciesAreInstalled(logicalVersionVector));
                        
                        if(!dependenciesAreInstalled)
                        {
                            break;
                        }
                        
                        inFlightReplication.Unblock();
                        
                        if(inFlightReplication.BlockedOnNeighbouringPartitions())
                        {
                            inFlightReplication.DispatchGlobalNeighbourPartitionDependencyChecks();
                            break;
                        }
                        
                        ApplyGlobalSpaceReplication(sourceReplicaId);
                    }
                }
                
                TLogger& m_logger;
                TStorage& m_storage;
                TSerializer& m_serializer;
                TInFlightReplicationQueue m_replicationQueues[TProtocolTraits::NumReplicas];
                TInFlightGlobalSpaceReplicationQueue m_globalReplicationQueues[TProtocolTraits::NumReplicas];
                TPartitionServer& m_partitionServer;
                TNetworkExchange& m_networkExchange;
                TReplicaUpdateSpace& m_replicaUpdateSpace;
                TDelayTracking& m_delayTracking;
                
                size_t m_replicationsReceivedToLocalSpace;
                size_t m_replicationsReceivedToGlobalSpace;
                
                size_t m_replicationsAppliedToLocalSpace;
                size_t m_replicationsAppliedToGlobalSpace;
            };
        }
    }
}
