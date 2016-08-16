//
//  OrbeSimulation.h
//  SimRunner
//
//  Created by Scott on 05/10/2014.
//
//

#pragma once

#include <memory>
#include <boost/noncopyable.hpp>
#include "Orbe.h"
#include "SimRunnerAssert.h"
#include "OrbeNetworkCompleteHandlerProxy.h"
#include "OrbeNetworkOperationStateRequestCompleteHandlerProxy.h"
#include "OrbeReplicateMessageBundleRequestCompleteHandlerProxy.h"
#include "OrbePartitionServerToLinkMapping.h"
#include "OrbeNetworkGetOperationState.h"
#include "OrbeNetworkPutOperationState.h"
#include "OrbeNeighbourPartitionDependencyCheckOperationState.h"
#include "OrbeNeighbourPartitionDependencyResponseOperationState.h"
#include "OrbeReplicateMessageBundle.h"
#include "OrbePartitionServerReplicationHandler.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            namespace SimulatedComponents
            {
                template <typename TSchedule, typename TProtocolTraits>
                class OrbeSimulatedExchange : private boost::noncopyable
                {
                    typedef typename TProtocolTraits::TStorageKey TKey;
                    typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                    typedef typename TProtocolTraits::TNetworkLink TNetworkLink;
                    typedef typename TProtocolTraits::TClient TClient;
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
                    
                    typedef OrbePartitionServerReplicationHandler<TProtocolTraits> TOrbePartitionServerReplicationHandler;
                    typedef OrbeReplicateMessageBundle<TProtocolTraits> TReplicateMessageBundle;
                    typedef OrbePartitionServerToLinkMapping<TPartitionServer, TNetworkLink> TPartitionServerToLinkMapping;
                    
                public:
                    OrbeSimulatedExchange(const size_t partitionsCount,
                                          const size_t replicasCount,
                                          TSchedule& schedule,
                                          TMetrics& metrics,
                                          TLogger& logger)
                    : m_schedule(schedule)
                    , m_metrics(metrics)
                    , m_logger(logger)
                    , m_partitionServers(std::unique_ptr<TPartitionServerToLinkMapping[]>(new TPartitionServerToLinkMapping[partitionsCount * replicasCount]))
                    , m_partitionsCount(partitionsCount)
                    , m_replicasCount(replicasCount)
                    {
                        
                    }
                    
                    void AddPartitionServer(TPartitionServer& partitionServer)
                    {
                        const size_t index = GetPartitionServerIndex(partitionServer.ReplicaId(), partitionServer.PartitionId());
                        TPartitionServerToLinkMapping& mapping = m_partitionServers[index];
                        SR_ASSERT(!mapping.IsValid());
                        mapping.Initialise(partitionServer, m_schedule);
                    }
                    
                    void SendReplicationMessageToReplica(const ReplicaIdType& senderReplicaId,
                                                         const ReplicaIdType& receiverReplicaId,
                                                         const PartitionIdType& mutualPartitionId,
                                                         const TReplicationMessageType& replicateMessage)
                    {
                        TPartitionServer* pSenderPartitionServer = nullptr;
                        TNetworkLink* pSenderLink = nullptr;
                        GetPartitionServerLinkPair(senderReplicaId, mutualPartitionId, pSenderPartitionServer, pSenderLink);
                        
                        TPartitionServer* pReceiverPartitionServer = nullptr;
                        TNetworkLink* pReceiverLink = nullptr;
                        GetPartitionServerLinkPair(receiverReplicaId, mutualPartitionId, pReceiverPartitionServer, pReceiverLink);
                        
                        SR_ASSERT(pSenderPartitionServer->ReplicaId() != pReceiverPartitionServer->ReplicaId());
                        SR_ASSERT(pSenderPartitionServer->PartitionId() == pReceiverPartitionServer->PartitionId());
                        
                        TReplicateMessageBundle::Dispatch(*pReceiverPartitionServer,
                                                          replicateMessage,
                                                          *this,
                                                          *pSenderLink);
                    }
                    
                    void PerformGetAtLocalPartition(TClient& client,
                                                    const TKey& key,
                                                    const ReplicaIdType& mutualReplicaId,
                                                    const PartitionIdType& requestDestinationPartitionId,
                                                    TDependencyMatrix& dependencyMatrix)
                    {
                        TPartitionServer* pPartitionServer = nullptr;
                        TNetworkLink* pLink = nullptr;
                        GetPartitionServerLinkPair(mutualReplicaId, requestDestinationPartitionId, pPartitionServer, pLink);
                        typedef OrbeNetworkGetOperationState<TProtocolTraits, TClient> TOp;
                        TOp::DispatchGet(key, *pPartitionServer, *pLink, *this, client, m_logger, dependencyMatrix);
                    }
                    
                    void PerformPutAtLocalPartition(TClient& client,
                                                    const TKey& key,
                                                    const TClientInputValueType& value,
                                                    const ReplicaIdType& mutualReplicaId,
                                                    const PartitionIdType& requestDestinationPartitionId,
                                                    TDependencyMatrix& dependencyMatrix)
                    {
                        TPartitionServer* pPartitionServer = nullptr;
                        TNetworkLink* pLink = nullptr;
                        GetPartitionServerLinkPair(mutualReplicaId, requestDestinationPartitionId, pPartitionServer, pLink);
                        typedef OrbeNetworkPutOperationState<TProtocolTraits, TClient> TOp;
                        TOp::DispatchPut(key, value, *pPartitionServer, *pLink, *this, client, m_logger, dependencyMatrix);
                    }
                    
                    void RequestIsNeighbourVersionVectorGreaterThanOrEqualToReplicationDependencyVector(PartitionIdType localPartitionId,
                                                                                                        ReplicaIdType localReplicaId,
                                                                                                        PartitionIdType neighbourPartitionId,
                                                                                                        TVersionVector vectorToVerify,
                                                                                                        ReplicaIdType sourceReplicaId)
                    {
                        TPartitionServer* pPartitionServer = nullptr;
                        TNetworkLink* pLink = nullptr;
                        GetPartitionServerLinkPair(localReplicaId, neighbourPartitionId, pPartitionServer, pLink);
                        typedef OrbeNeighbourPartitionDependencyCheckOperationState<TProtocolTraits, TOrbePartitionServerReplicationHandler> TOp;
                        
                        TOp::DispatchRequest(*pPartitionServer,
                                             *pLink,
                                             localPartitionId,
                                             sourceReplicaId,
                                             *this,
                                             m_logger,
                                             vectorToVerify);
                    }
                    
                    void SendUnblockingVersionVectorDependencyCheckResponse(ReplicaIdType localReplicaId,
                                                                            PartitionIdType neighbourPartitionId,
                                                                            ReplicaIdType sourceReplicaId)
                    {
                        TPartitionServer* pPartitionServer = nullptr;
                        TNetworkLink* pLink = nullptr;
                        GetPartitionServerLinkPair(localReplicaId, neighbourPartitionId, pPartitionServer, pLink);
                        typedef OrbeNeighbourPartitionDependencyResponseOperationState<TProtocolTraits, TOrbePartitionServerReplicationHandler> TOp;
                        
                        TOp::DispatchRequest(*pPartitionServer,
                                             *pLink,
                                             sourceReplicaId,
                                             *this,
                                             m_logger);
                    }
                    
                    template <typename TRequestMetaData>
                    void HandleLinkOperationComplete(TRequestMetaData& operationState)
                    {
                        operationState.Next();
                    }
                    
                    void HandleLinkOperationComplete(OrbeReplicateMessageBundleRequestCompleteHandlerProxy<TReplicateMessageBundle>& replicationMessageBundle)
                    {
                        replicationMessageBundle.Finish();
                    }
                    
                private:
                    size_t GetPartitionServerIndex(const ReplicaIdType& replicaId,
                                                   const PartitionIdType& partitionId) const
                    {
                        return (replicaId * m_partitionsCount) + partitionId;
                    }
                    
                    void GetPartitionServerLinkPair(const ReplicaIdType& replicaId,
                                                    const PartitionIdType& partitionId,
                                                    TPartitionServer*& out_pPartitionServer,
                                                    TNetworkLink*& out_pLink) const
                    {
                        const size_t index = GetPartitionServerIndex(replicaId, partitionId);
                        TPartitionServerToLinkMapping& mapping = m_partitionServers[index];
                        SR_ASSERT(mapping.IsValid());
                        out_pPartitionServer = &(mapping.PartitionServer());
                        out_pLink = &(mapping.Link());
                    }
                    
                    TSchedule& m_schedule;
                    TMetrics& m_metrics;
                    TLogger& m_logger;
                    std::unique_ptr<TPartitionServerToLinkMapping[]> m_partitionServers;
                    const size_t m_partitionsCount;
                    const size_t m_replicasCount;
                };
            }
        }
    }
}
