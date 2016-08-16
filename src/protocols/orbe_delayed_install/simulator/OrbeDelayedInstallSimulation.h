//
//  OrbeDelayedInstallSimulation.h
//  SimRunner
//
//  Created by Scott on 05/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include <random>
#include <memory>
#include <vector>
#include "OrbeDelayedInstall.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            template<
            typename TStorage,
            typename TSchedule,
            typename TKeyGenerator,
            typename TKeyToPartitionMapper,
            typename TRandomEngine,
            typename TLogger,
            typename TMetrics,
            typename TDelayTracking,
            typename TClientInputValueType,
            typename TClientGetReplyValueType,
            typename TClientPutReplyValueType,
            typename TProtocolTraits
            >
            class OrbeDelayedInstallSimulation
            : public std::enable_shared_from_this<typename TProtocolTraits::TProtocol>
            , private boost::noncopyable
            {
                typedef typename TProtocolTraits::TProtocol TSelf;
                typedef typename TProtocolTraits::TProtocolPtr TSelfPtr;
                
                typedef typename TProtocolTraits::TNetworkExchange TNetworkExchange;
                typedef typename TProtocolTraits::TClient TClient;
                typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                typedef typename TProtocolTraits::TStorageComponent TStorageComponent;
                typedef typename TProtocolTraits::TBroadcaster TBroadcaster;
                typedef typename TProtocolTraits::TSerializer TSerializer;
                typedef typename TProtocolTraits::TClientUpdateSpace TClientUpdateSpace;
                typedef typename TProtocolTraits::TReplicaUpdateSpace TReplicaUpdateSpace;
                
                typedef SimRunner::SimulationInfrastructure::SimulationEvent TSimEvent;
                
            public:
                static TSelfPtr Create(size_t replicasCount,
                                       size_t partitionsCount,
                                       size_t clientsPerPartitionCount,
                                       double putToGetRatio,
                                       TSchedule& schedule,
                                       TKeyGenerator& keyGenerator,
                                       TKeyToPartitionMapper& keyToPartitionMapper,
                                       TNetworkExchange& networkExchange,
                                       TSerializer& serializer,
                                       TRandomEngine& randomEngine,
                                       TLogger& logger,
                                       TMetrics& metrics,
                                       TDelayTracking& delayTracking)
                {
                    TSelfPtr pSelf(new TSelf(replicasCount,
                                             partitionsCount,
                                             clientsPerPartitionCount,
                                             putToGetRatio,
                                             schedule,
                                             keyGenerator,
                                             keyToPartitionMapper,
                                             networkExchange,
                                             serializer,
                                             randomEngine,
                                             logger,
                                             metrics,
                                             delayTracking));
                    
                    pSelf->Start();
                    
                    return pSelf;
                }
                
                void PerformOperation()
                {
                    //static int m_operationCount = 0;
                    //m_logger.Log("PerformOperation %d\n", ++ m_operationCount);
                    
                    for(auto& pClient : m_clients)
                    {
                        TClient& client = *pClient;
                        
                        if(!client.IsBusy())
                        {
                            GenerateEvent(client);
                        }
                        else
                        {
                            m_logger.Log("client %d busy, %p\n", client.ClientId(), &client);
                        }
                    }
                }
                
                void End()
                {
                    printf("\n\nStats!\n\n");
                    
                    for(auto it = m_partitionServers.begin(); it != m_partitionServers.end(); ++ it)
                    {
                        auto& partitionServer(it->PartitionServer());
                        partitionServer.DumpStats();
                        printf("\n");
                    }
                }
                
                void HandleGetCompleteItemFound(const TClientGetReplyValueType& getReply)
                {
                    m_logger.Log("OrbeDelayedInstallSimulation::HandleGetCompleteItemFound: %d, %llu, %d\n",
                                 getReply.Value(),
                                 getReply.Timestamp(),
                                 getReply.SourceReplicaId());
                }
                
                void HandleGetCompleteItemNotFound()
                {
                    //m_logger.Log("OrbeDelayedInstallSimulation::HandleGetCompleteItemNotFound\n");
                }
                
                void HandlePutComplete(const TClientPutReplyValueType& putReply)
                {
                    m_logger.Log("OrbeDelayedInstallSimulation::HandlePutComplete: %llu, %d\n",
                                 putReply.Timestamp(),
                                 putReply.SourceReplicaId());
                }
                
            private:
                OrbeDelayedInstallSimulation(size_t replicasCount,
                                             size_t partitionsCount,
                                             size_t clientsPerPartitionCount,
                                             double putToGetRatio,
                                             TSchedule& schedule,
                                             TKeyGenerator& keyGenerator,
                                             TKeyToPartitionMapper& keyToPartitionMapper,
                                             TNetworkExchange& networkExchange,
                                             TSerializer& serializer,
                                             TRandomEngine& randomEngine,
                                             TLogger& logger,
                                             TMetrics& metrics,
                                             TDelayTracking& delayTracking)
                : m_replicasCount(replicasCount)
                , m_partitionsCount(partitionsCount)
                , m_clientsPerPartitionCount(clientsPerPartitionCount)
                , m_putToGetRatio(putToGetRatio)
                , m_schedule(schedule)
                , m_keyGenerator(keyGenerator)
                , m_keyToPartitionMapper(keyToPartitionMapper)
                , m_networkExchange(networkExchange)
                , m_serializer(serializer)
                , m_randomEngine(randomEngine)
                , m_logger(logger)
                , m_metrics(metrics)
                , m_delayTracking(delayTracking)
                , m_distribution(0.0, 1.0)
                , m_nextClientId(1)
                {
                    
                }
                
                void Start()
                {
                    for(Protocols::ReplicaIdType r = 0; r < m_replicasCount; ++ r)
                    {
                        for(Protocols::PartitionIdType p = 0; p < m_partitionsCount; ++ p)
                        {
                            CreatePartitionServerInstance(r, p);
                        }
                    }
                    
                    ScheduleHeartbeat(1500, -1);
                }
                
                void GenerateEvent(TClient& client)
                {
                    m_eventCounter += 1;
                    Keys::KeyType key = m_keyGenerator.NextKey();
                    
                    if(OperationIsRead())
                    {
                        GenerateRead(key, client);
                    }
                    else
                    {
                        GenerateWrite(key, client);
                    }
                }
                
                void GenerateRead(const Keys::KeyType& key, TClient& client)
                {
                    //m_logging.Log("Adding client operation: %s -- read", m_eventCounter);
                    client.IssueGet(key);
                }
                
                void GenerateWrite(const Keys::KeyType& key, TClient& client)
                {
                    //m_logging.Log("Adding client operation: %s -- write", m_eventCounter);
                    
                    TClientInputValueType putValue(7);
                    client.IssuePut(key, putValue);
                }
                
                bool OperationIsRead()
                {
                    double random = m_distribution(m_randomEngine);
                    return random >= m_putToGetRatio;
                }
                
                void ScheduleHeartbeat(size_t millisecondsDelay, size_t eventCounterLastHeartbeat)
                {
                    bool noMoreEvents = eventCounterLastHeartbeat == m_eventCounter;
                    eventCounterLastHeartbeat = m_eventCounter;
                    
                    if(!m_schedule.HasEvents() && noMoreEvents)
                    {
                        return;
                    }
                    
                    for(auto it = m_partitionServers.begin(); it != m_partitionServers.end(); ++ it)
                    {
                        auto& partitionServer(it->PartitionServer());
                        partitionServer.SendHeartbeatVector();
                    }
                    
                    boost::function<void ()> binding = boost::bind(&OrbeDelayedInstallSimulation::ScheduleHeartbeat,
                                                                   this,
                                                                   millisecondsDelay,
                                                                   eventCounterLastHeartbeat);
                    
                    TSimEvent event = m_schedule.BuildSimulationEventTimeStampedNow(binding, "OrbeDelayedInstallSimulation");
                    m_schedule.InsertSimulationEvent(event, millisecondsDelay * 1000);
                }
                
                
                typedef std::unique_ptr<TStorage> TStoragePtr;
                typedef std::unique_ptr<TStorageComponent> TStorageComponentPtr;
                typedef std::unique_ptr<TBroadcaster> TBroadcasterPtr;
                typedef std::unique_ptr<TPartitionServer> TPartitionServerPtr;
                typedef std::unique_ptr<TClient> TClientPtr;
                typedef std::vector<TClientPtr> TClientVec;
                
                class PartitionServerComponents
                {
                public:
                    PartitionServerComponents(TStoragePtr storage,
                                              TStorageComponentPtr storageComponent,
                                              TBroadcasterPtr broadcaster,
                                              TPartitionServerPtr partitionServer)
                    : m_storage(std::move(storage))
                    , m_storageComponent(std::move(storageComponent))
                    , m_broadcaster(std::move(broadcaster))
                    , m_partitionServer(std::move(partitionServer))
                    {
                        
                    }
                    
                    TPartitionServer& PartitionServer() const { return *(m_partitionServer.get()); }
                    
                private:
                    TStoragePtr m_storage;
                    TStorageComponentPtr m_storageComponent;
                    TBroadcasterPtr m_broadcaster;
                    TPartitionServerPtr m_partitionServer;
                };
                
                void CreatePartitionServerInstance(const Protocols::ReplicaIdType& replicaId,
                                                   const Protocols::PartitionIdType& partitionId)
                {
                    TStoragePtr storage(new TStorage());
                    TStorageComponentPtr storageComponent(new TStorageComponent("simulated storage test", m_schedule, *storage));
                    TBroadcasterPtr broadcaster(new TBroadcaster(replicaId, partitionId, m_networkExchange, m_metrics));
                    
                    for(Protocols::ReplicaIdType otherReplicaId = 0; otherReplicaId < m_replicasCount; ++ otherReplicaId)
                    {
                        if(otherReplicaId != replicaId)
                        {
                            broadcaster->InsertLinkToRemoteReplica(otherReplicaId);
                        }
                    }
                    
                    for(Protocols::PartitionIdType otherPartitionId = 0; otherPartitionId < m_partitionsCount; ++ otherPartitionId)
                    {
                        if(otherPartitionId != partitionId)
                        {
                            broadcaster->InsertLinkToLocalPartitionNeighbour(otherPartitionId);
                        }
                    }
                    
                    TPartitionServerPtr partitionServer(new TPartitionServer(replicaId,
                                                                             partitionId,
                                                                             *storageComponent,
                                                                             m_networkExchange,
                                                                             *broadcaster,
                                                                             m_serializer,
                                                                             *(new TClientUpdateSpace()),
                                                                             *(new TReplicaUpdateSpace()),
                                                                             m_metrics,
                                                                             m_delayTracking,
                                                                             m_logger));
                    
                    m_partitionServers.push_back(PartitionServerComponents(std::move(storage),
                                                                           std::move(storageComponent),
                                                                           std::move(broadcaster),
                                                                           std::move(partitionServer)));
                    
                    m_networkExchange.AddPartitionServer(m_partitionServers.back().PartitionServer());
                    
                    for(size_t c = 0; c < m_clientsPerPartitionCount; ++ c)
                    {
                        m_clients.push_back(TClientPtr(new TClient(m_nextClientId ++,
                                                                   m_keyToPartitionMapper,
                                                                   m_networkExchange,
                                                                   m_partitionServers.back().PartitionServer(),
                                                                   std::enable_shared_from_this<TSelf>::shared_from_this(),
                                                                   m_metrics,
                                                                   m_logger)));
                    }
                }
                
                const size_t m_replicasCount;
                const size_t m_partitionsCount;
                const size_t m_clientsPerPartitionCount;
                const double m_putToGetRatio;
                TSchedule& m_schedule;
                TKeyGenerator& m_keyGenerator;
                TKeyToPartitionMapper& m_keyToPartitionMapper;
                TNetworkExchange& m_networkExchange;
                TSerializer& m_serializer;
                TRandomEngine& m_randomEngine;
                TLogger& m_logger;
                TMetrics& m_metrics;
                TDelayTracking& m_delayTracking;
                
                std::uniform_real_distribution<double> m_distribution;
                
                Protocols::ClientIdType m_nextClientId;
                size_t m_eventCounter;
                size_t m_completedEventCounter;
                
                std::vector<PartitionServerComponents> m_partitionServers;
                TClientVec m_clients;
            };
        }
    }
}
