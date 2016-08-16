//
//  OrbeSimulation.h
//  SimRunner
//
//  Created by Scott on 05/10/2014.
//
//

#pragma once

#include <memory>
#include <vector>
#include <string>
#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include "Orbe.h"
#include "Host.h"
#include "Protocols.h"
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            namespace Deployment
            {
                namespace Server
                {
                    namespace Devices
                    {
                        template <
                        typename TKey,
                        typename TSerializedValueType,
                        typename TDeserializedValueType,
                        typename TClientInputValueType,
                        typename TClientGetReplyValueType,
                        typename TClientPutReplyValueType,
                        typename TReplicationMessageType,
                        typename TProtocolTraits>
                        class OrbeServerExchange : private boost::noncopyable
                        {
                            typedef typename TProtocolTraits::TMetrics TMetrics;
                            typedef typename TProtocolTraits::TLogger TLogger;
                            typedef typename TProtocolTraits::TKeyToPartitionMapper TKeyToPartitionMapper;
                            typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                            typedef typename TProtocolTraits::TClient TClient;
                            typedef typename TProtocolTraits::TBroadcaster TBroadcaster;
                            typedef typename TProtocolTraits::TDependencyMatrix TDependencyMatrix;
                            typedef typename TProtocolTraits::TVersionVector TVersionVector;
                            
                            typedef typename TProtocolTraits::TPartitionConnector TPartitionConnector;
                            typedef typename TProtocolTraits::TPartitionConnectionFactory TPartitionConnectionFactory;
                            typedef std::unique_ptr<TPartitionConnectionFactory> PartitionConnectionFactoryPtr;
                            typedef typename TProtocolTraits::TPartitionConnectionListener TPartitionConnectionListener;
                            typedef std::unique_ptr<TPartitionConnectionListener> PartitionConnectionListenerPtr;
                            typedef typename TProtocolTraits::TPartitionConnection TPartitionConnection;
                            
                            typedef typename TProtocolTraits::TReplicaConnector TReplicaConnector;
                            typedef typename TProtocolTraits::TReplicaConnectionFactory TReplicaConnectionFactory;
                            typedef std::unique_ptr<TReplicaConnectionFactory> ReplicaConnectionFactoryPtr;
                            typedef typename TProtocolTraits::TReplicaConnectionListener TReplicaConnectionListener;
                            typedef std::unique_ptr<TReplicaConnectionListener> ReplicaConnectionListenerPtr;
                            typedef typename TProtocolTraits::TReplicaConnection TReplicaConnection;
                            
                            typedef typename TProtocolTraits::TClientConnectionFactory TClientConnectionFactory;
                            typedef std::unique_ptr<TClientConnectionFactory> ClientConnectionFactoryPtr;
                            typedef typename TProtocolTraits::TClientConnectionListener TClientConnectionListener;
                            typedef std::unique_ptr<TClientConnectionListener> ClientConnectionListenerPtr;
                            typedef typename TProtocolTraits::TClientConnection TClientConnection;
                            
                            typedef OrbeServerExchange<
                            TKey,
                            TSerializedValueType,
                            TDeserializedValueType,
                            TClientInputValueType,
                            TClientGetReplyValueType,
                            TClientPutReplyValueType,
                            TReplicationMessageType,
                            TProtocolTraits> TContext;
                            
                        public:
                            
                            OrbeServerExchange(const std::string& serverName,
                                               PartitionIdType partitionId,
                                               ReplicaIdType replicaId,
                                               uint16_t partitionListenPort,
                                               uint16_t replicaListenPort,
                                               uint16_t clientListenPort,
                                               std::vector<Utilities::Host> partitionHosts,
                                               std::vector<Utilities::Host> replicaHosts,
                                               TKeyToPartitionMapper& keyToPartitionMapper,
                                               TPartitionConnector& partitionConnector,
                                               TReplicaConnector& replicaConnector,
                                               TMetrics& metrics,
                                               TLogger& logger,
                                               bool localTestRun)
                            : m_serverName(serverName)
                            , m_replicaId(replicaId)
                            , m_partitionId(partitionId)
                            , m_partitionListenPort(partitionListenPort)
                            , m_replicaListenPort(replicaListenPort)
                            , m_clientListenPort(clientListenPort)
                            , m_partitionHostDescriptors(partitionHosts)
                            , m_replicaHostDescriptors(replicaHosts)
                            , m_keyToPartitionMapper(keyToPartitionMapper)
                            , m_partitionConnector(partitionConnector)
                            , m_replicaConnector(replicaConnector)
                            , m_metrics(metrics)
                            , m_logger(logger)
                            , m_localTestRun(localTestRun)
                            , m_partitionConnections(partitionHosts.size())
                            , m_replicaConnections(replicaHosts.size())
                            {
                                
                            }
                            
                            void SendReplicationMessageToReplica(const ReplicaIdType& senderReplicaId,
                                                                 const ReplicaIdType& receiverReplicaId,
                                                                 const PartitionIdType& mutualPartitionId,
                                                                 const TReplicationMessageType& replicateMessage)
                            {
                                SR_ASSERT(m_replicaConnections[receiverReplicaId] != nullptr);
                                TReplicaConnection& connection = *(m_replicaConnections[receiverReplicaId]);
                                connection.SendReplicationMessageToReplica(replicateMessage);
                            }
                            
                            void PerformGetAtLocalPartition(TClient& client,
                                                            const TKey& key,
                                                            const ReplicaIdType& mutualReplicaId,
                                                            const PartitionIdType& requestDestinationPartitionId,
                                                            TDependencyMatrix& dependencyMatrix)
                            {
                                SR_ASSERT(m_partitionConnections[requestDestinationPartitionId] != nullptr);
                                TPartitionConnection& connection = *(m_partitionConnections[requestDestinationPartitionId]);
                                connection.PerformGetForNeighbour(client, key);
                            }
                            
                            void PerformPutAtLocalPartition(TClient& client,
                                                            const TKey& key,
                                                            const TClientInputValueType& value,
                                                            const ReplicaIdType& mutualReplicaId,
                                                            const PartitionIdType& requestDestinationPartitionId,
                                                            TDependencyMatrix& dependencyMatrix)
                            {
                                SR_ASSERT(m_partitionConnections[requestDestinationPartitionId] != nullptr);
                                TPartitionConnection& connection = *(m_partitionConnections[requestDestinationPartitionId]);
                                connection.PerformPutForNeighbour(client, key, value, dependencyMatrix);
                            }
                            
                            void RequestIsNeighbourVersionVectorGreaterThanOrEqualToReplicationDependencyVector(PartitionIdType localPartitionId,
                                                                                                                ReplicaIdType localReplicaId,
                                                                                                                PartitionIdType requestDestinationPartitionId,
                                                                                                                TVersionVector vectorToVerify,
                                                                                                                ReplicaIdType sourceReplicaId)
                            {
                                SR_ASSERT(m_partitionConnections[requestDestinationPartitionId] != nullptr);
                                TPartitionConnection& connection = *(m_partitionConnections[requestDestinationPartitionId]);
                                connection.RequestIsNeighbourVersionVectorGreaterThanOrEqualToReplicationDependencyVector(localPartitionId,
                                                                                                                          sourceReplicaId,
                                                                                                                          vectorToVerify);
                            }
                            
                            
                            void SendUnblockingVersionVectorDependencyCheckResponse(ReplicaIdType localReplicaId,
                                                                                    PartitionIdType requestDestinationPartitionId,
                                                                                    ReplicaIdType sourceReplicaId)
                            {
                                SR_ASSERT(m_partitionConnections[requestDestinationPartitionId] != nullptr);
                                TPartitionConnection& connection = *(m_partitionConnections[requestDestinationPartitionId]);
                                connection.SendUnblockingVersionVectorDependencyCheckResponse(sourceReplicaId);
                            }
                            
                            void Bootstrap(TPartitionServer& partitionServer,
                                           TBroadcaster& broadcaster,
                                           boost::asio::io_service& ioService)
                            {
                                BootstrapPartitions(partitionServer, broadcaster, ioService);
                                
                                BootstrapReplicas(partitionServer, broadcaster, ioService);
                                
                                InitialiseClientConnectionListener(partitionServer, ioService);
                            }
                            
                            void RegisterConnectionToNeighbourServer(PartitionIdType partitionId, TPartitionConnection& connection)
                            {
                                printf("INSERTING CONNECTION TO PARTITION SERVER %d AT HOST %s.\n",
                                       partitionId,
                                       m_serverName.c_str());
                                
                                /*SR_ASSERT(m_partitionConnections[partitionId] == nullptr,
                                          "Double connection to partition server %d at host %s, already connected to %s.\n",
                                          partitionId,
                                          m_serverName.c_str(),
                                          m_partitionConnections[partitionId]->ConnectedHostAddress().c_str());*/
                                
                                m_partitionConnections[partitionId] = &connection;
                            }
                            
                            void RegisterConnectionToReplicaServer(ReplicaIdType replicaId, TReplicaConnection& connection)
                            {
                                printf("INSERTING CONNECTION TO REPLICA SERVER %d AT HOST %s.\n",
                                       replicaId,
                                       m_serverName.c_str());
                                
                                /*SR_ASSERT(m_replicaConnections[replicaId] == nullptr,
                                          "Double connection to replica server %d at host %s, already connected to %s\n.",
                                          replicaId,
                                          m_serverName.c_str(),
                                          m_replicaConnections[replicaId]->ConnectedHostAddress().c_str());*/
                                
                                m_replicaConnections[replicaId] = &connection;
                            }
                            
                        private:
                            void BootstrapPartitions(TPartitionServer& partitionServer,
                                                     TBroadcaster& broadcaster,
                                                     boost::asio::io_service& ioService)
                            {
                                const uint16_t partitionListenPort(GetPartitionPort(m_partitionId, m_replicaId));
                                
                                m_partitionConnectionFactory = PartitionConnectionFactoryPtr(new TPartitionConnectionFactory(partitionServer,
                                                                                                                             broadcaster,
                                                                                                                             *this,
                                                                                                                             ioService,
                                                                                                                             m_logger));
                                
                                m_partitionConnectionListener = PartitionConnectionListenerPtr(new TPartitionConnectionListener(ioService,
                                                                                                                                partitionListenPort,
                                                                                                                                *m_partitionConnectionFactory));
                                
                                for(PartitionIdType p = 0; p < m_partitionHostDescriptors.size(); ++ p)
                                {
                                    if(p != m_partitionId)
                                    {
                                        const Utilities::Host& host(m_partitionHostDescriptors[p]);
                                        const uint16_t partitionConnectPort(GetPartitionPort(p, m_replicaId));
                                        
                                        m_partitionConnector.TryConnectSynchronous(*m_partitionConnectionFactory,
                                                                                   ioService,
                                                                                   host.HostName(),
                                                                                   partitionConnectPort);
                                    }
                                }
                            }
                            
                            void BootstrapReplicas(TPartitionServer& partitionServer,
                                                   TBroadcaster& broadcaster,
                                                   boost::asio::io_service& ioService)
                            {
                                const uint16_t replicaListenPort(GetReplicaPort(m_partitionId, m_replicaId));
                                
                                m_replicaConnectionFactory = ReplicaConnectionFactoryPtr(new TReplicaConnectionFactory(partitionServer,
                                                                                                                       broadcaster,
                                                                                                                       *this,
                                                                                                                       ioService,
                                                                                                                       m_logger));
                                
                                m_replicaConnectionListener = ReplicaConnectionListenerPtr(new TReplicaConnectionListener(ioService,
                                                                                                                          replicaListenPort,
                                                                                                                          *m_replicaConnectionFactory));
                                
                                for(ReplicaIdType r = 0; r < m_replicaHostDescriptors.size(); ++ r)
                                {
                                    if(r != m_replicaId)
                                    {
                                        const Utilities::Host& host(m_replicaHostDescriptors[r]);
                                        const uint16_t replicaConnectPort(GetReplicaPort(m_partitionId, r));
                                        
                                        m_replicaConnector.TryConnectSynchronous(*m_replicaConnectionFactory,
                                                                                 ioService,
                                                                                 host.HostName(),
                                                                                 replicaConnectPort);
                                    }
                                }
                            }
                            
                            void InitialiseClientConnectionListener(TPartitionServer& partitionServer,
                                                                    boost::asio::io_service& ioService)
                            {
                                const uint16_t clientListenPort(GetClientPort(m_partitionId, m_replicaId));
                                
                                m_clientConnectionFactory = ClientConnectionFactoryPtr(new TClientConnectionFactory(m_keyToPartitionMapper,
                                                                                                                    partitionServer,
                                                                                                                    *this,
                                                                                                                    ioService,
                                                                                                                    m_metrics,
                                                                                                                    m_logger));
                                
                                m_clientConnectionListener = ClientConnectionListenerPtr(new TClientConnectionListener(ioService,
                                                                                                                       clientListenPort,
                                                                                                                       *m_clientConnectionFactory));
                            }
                            
                            uint16_t GetPartitionPort(PartitionIdType partitionId, ReplicaIdType replicaId)
                            {
                                return GetPort(m_partitionListenPort, partitionId, replicaId);
                            }
                            
                            uint16_t GetReplicaPort(PartitionIdType partitionId, ReplicaIdType replicaId)
                            {
                                return GetPort(m_replicaListenPort, partitionId, replicaId);
                            }
                            
                            uint16_t GetClientPort(PartitionIdType partitionId, ReplicaIdType replicaId)
                            {
                                return GetPort(m_clientListenPort, partitionId, replicaId);
                            }
                            
                            uint16_t GetPort(uint16_t basePort, PartitionIdType partitionId, ReplicaIdType replicaId)
                            {
                                if(m_localTestRun)
                                {
                                    return basePort + partitionId + (replicaId * 100);
                                }
                                else
                                {
                                    return basePort;
                                }
                            }
                            
                            std::string m_serverName;
                            ReplicaIdType m_replicaId;
                            PartitionIdType m_partitionId;
                            uint16_t m_partitionListenPort;
                            uint16_t m_replicaListenPort;
                            uint16_t m_clientListenPort;
                            const std::vector<Utilities::Host> m_partitionHostDescriptors;
                            const std::vector<Utilities::Host> m_replicaHostDescriptors;
                            TKeyToPartitionMapper& m_keyToPartitionMapper;
                            TPartitionConnector& m_partitionConnector;
                            TReplicaConnector& m_replicaConnector;
                            TMetrics& m_metrics;
                            TLogger& m_logger;
                            const bool m_localTestRun;
                            
                            PartitionConnectionFactoryPtr m_partitionConnectionFactory;
                            PartitionConnectionListenerPtr m_partitionConnectionListener;
                            std::vector<TPartitionConnection*> m_partitionConnections;
                            
                            ReplicaConnectionFactoryPtr m_replicaConnectionFactory;
                            ReplicaConnectionListenerPtr m_replicaConnectionListener;
                            std::vector<TReplicaConnection*> m_replicaConnections;
                            
                            ClientConnectionFactoryPtr m_clientConnectionFactory;
                            ClientConnectionListenerPtr m_clientConnectionListener;
                        };
                    }
                }
            }
        }
    }
}

