//
//  OrbeDelayedInstallReplicaConnection.h
//  SimRunner
//
//  Created by Scott on 20/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <cstdlib>
#include <iostream>
#include <memory>
#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include "OrbeDelayedInstallWire.h"
#include "Connection.h"
#include "OrbeDelayedInstallReplicate.h"
#include "OrbeDelayedInstallReplicaIdentityExchange.h"
#include "OrbeDelayedInstallReplicaHeartbeat.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            namespace Deployment
            {
                namespace Server
                {
                    namespace Neighbours
                    {
                        template<typename ProtocolTraits>
                        class OrbeDelayedInstallReplicaConnection : public Utilities::Connection<
                        ProtocolTraits,
                        OrbeDelayedInstallReplicaConnection<ProtocolTraits>,
                        Wire::OrbeDelayedInstallMessageHeaderBytes
                        >, private boost::noncopyable
                        {
                            typedef OrbeDelayedInstallReplicaConnection<ProtocolTraits> TSelf;
                            typedef typename Utilities::Connection<ProtocolTraits, TSelf, Wire::OrbeDelayedInstallMessageHeaderBytes> TBase;
                            typedef typename TBase::TLogger TLogger;
                            typedef typename ProtocolTraits::TPartitionServer TPartitionServer;
                            typedef typename ProtocolTraits::TNetworkExchange TNetworkExchange;
                            typedef typename ProtocolTraits::TClientGetReplyValueType TClientGetReplyValueType;
                            typedef typename ProtocolTraits::TClientPutReplyValueType TClientPutReplyValueType;
                            typedef typename ProtocolTraits::TReplicationMessageType TReplicationMessageType;
                            typedef typename ProtocolTraits::TBroadcaster TBroadcaster;
                            typedef typename ProtocolTraits::TVersionVector TVersionVector;
                            
                            typedef Wire::OrbeDelayedInstallReplicate<ProtocolTraits> OrbeDelayedInstallReplicate;
                            typedef Wire::OrbeDelayedInstallReplicaHeartbeat<ProtocolTraits> OrbeDelayedInstallReplicaHeartbeat;
                            typedef Wire::OrbeDelayedInstallReplicaIdentityExchange<ProtocolTraits> OrbeDelayedInstallReplicaIdentityExchange;
                            
                        public:
                            typedef typename TBase::TPtr TPtr;
                            
                            static TPtr Create(boost::asio::io_service& ioService,
                                               TPartitionServer& partitionServer,
                                               TBroadcaster& broadcaster,
                                               TNetworkExchange& exchange,
                                               TLogger& logger)
                            {
                                auto pResult = TPtr(new TSelf(ioService, partitionServer, broadcaster, exchange, logger));
                                pResult->SetupSharedPtrToSelf();
                                return pResult;
                            }
                            
                            void SendReplicationMessageToReplica(const TReplicationMessageType& replicateMessage)
                            {
                                OrbeDelayedInstallReplicate msg(OrbeDelayedInstallReplicate::Create(replicateMessage.Key(),
                                                                                                    replicateMessage.Value(),
                                                                                                    replicateMessage.Timestamp(),
                                                                                                    replicateMessage.SourceReplicaId(),
                                                                                                    replicateMessage.LogicalTimestamp(),
                                                                                                    replicateMessage.ItemDependencyTimestamp(),
                                                                                                    replicateMessage.ItemDependencyReplica(),
                                                                                                    replicateMessage.ClientId(),
                                                                                                    replicateMessage.ClientDependencyTimestamp(),
                                                                                                    replicateMessage.ClientDependencyPartition()));
                                
                                TBase::PostWrite(msg);
                            }
                            
                            void SendHeartbeatVectorMessageToReplica(const ReplicaIdType& senderReplicaId,
                                                                     const PartitionIdType& mutualPartitionId,
                                                                     const TVersionVector& versionVector)
                            {
                                OrbeDelayedInstallReplicaHeartbeat msg(OrbeDelayedInstallReplicaHeartbeat::Create(mutualPartitionId,
                                                                                                                  senderReplicaId,
                                                                                                                  versionVector));
                                
                                TBase::PostWrite(msg);
                            }
                            
                            void HandleConnectionEstablished()
                            {
                                m_logger.Log("OrbeDelayedInstallReplicaConnection::HandleConnectionEstablished\n");
                                
                                const auto replicaId(m_partitionServer.ReplicaId());
                                const auto msg(OrbeDelayedInstallReplicaIdentityExchange::Create(replicaId));
                                TBase::PostWrite(msg);
                            }
                            
                            void ProcessBufferedMessage(const Utilities::SubBuffer& messageBytes)
                            {
                                m_logger.Log("OrbeDelayedInstallReplicaConnection::ProcessBufferedMessage\n");
                                
                                const auto header = static_cast<Wire::OrbeDelayedInstallWireMessages>(messageBytes[0]);
                                
                                switch(header)
                                {
                                    case Wire::ReplicateMsg: {
                                        TBase::template ParseAndHandle<OrbeDelayedInstallReplicate>(messageBytes);
                                    } break;
                                    case Wire::ReplicaHeartbeatMsg: {
                                        TBase::template ParseAndHandle<OrbeDelayedInstallReplicaHeartbeat>(messageBytes);
                                    } break;
                                    case Wire::ReplicaIdentityExchangeMsg: {
                                        TBase::template ParseAndHandle<OrbeDelayedInstallReplicaIdentityExchange>(messageBytes);
                                    } break;
                                    default: {
                                        SR_ASSERT(false, "Replica role does not handle this message type.\n");
                                    }
                                }
                            }
                            
                            void Handle(const OrbeDelayedInstallReplicaIdentityExchange& message)
                            {
                                m_broadcaster.InsertLinkToRemoteReplica(message.ReplicaId());
                                m_exchange.RegisterConnectionToReplicaServer(message.ReplicaId(), *this);
                            }
                            
                            void Handle(const OrbeDelayedInstallReplicate& message)
                            {
                                TReplicationMessageType replicateMessage(message.Key(),
                                                                         message.Value(),
                                                                         message.LogicalTimestamp(),
                                                                         message.ItemDependencyReplica(),
                                                                         message.ItemDependencyTimestamp(),
                                                                         message.ClientId(),
                                                                         message.ClientDependencyTimestamp(),
                                                                         message.ClientDependencyPartition(),
                                                                         message.Timestamp(),
                                                                         message.SourceReplicaId(),
                                                                         m_partitionServer.PartitionId());
                                
                                m_partitionServer.ReceiveReplicateMessage(replicateMessage);
                            }
                            
                            void Handle(const OrbeDelayedInstallReplicaHeartbeat& message)
                            {
                                SR_ASSERT(message.SenderPartitionId() == m_partitionServer.PartitionId());
                                m_partitionServer.ReceiveHeartbeatVector(message.SourceReplicaId(), message.DependencyVersionVector());
                            }
                            
                        private:
                            OrbeDelayedInstallReplicaConnection(boost::asio::io_service& ioService,
                                                                TPartitionServer& partitionServer,
                                                                TBroadcaster& broadcaster,
                                                                TNetworkExchange& exchange,
                                                                TLogger& logger)
                            : Utilities::Connection<ProtocolTraits, TSelf, Wire::OrbeDelayedInstallMessageHeaderBytes>(ioService,
                                                                                                                       logger,
                                                                                                                       *this)
                            , m_partitionServer(partitionServer)
                            , m_broadcaster(broadcaster)
                            , m_exchange(exchange)
                            , m_logger(logger)
                            {
                                TBase::EnableLogging();
                            }
                            
                            TPartitionServer& m_partitionServer;
                            TBroadcaster& m_broadcaster;
                            TNetworkExchange& m_exchange;
                            TLogger& m_logger;
                        };
                    }
                }
            }
        }
    }
}

