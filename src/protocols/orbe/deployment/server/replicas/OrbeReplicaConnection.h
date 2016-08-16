//
//  OrbeReplicaConnection.h
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
#include "OrbeWire.h"
#include "Connection.h"
#include "OrbeReplicate.h"
#include "OrbeReplicaIdentityExchange.h"

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
                    namespace Neighbours
                    {
                        template<typename ProtocolTraits>
                        class OrbeReplicaConnection : public Utilities::Connection<
                        ProtocolTraits,
                        OrbeReplicaConnection<ProtocolTraits>,
                        Wire::OrbeMessageHeaderBytes
                        >, private boost::noncopyable
                        {
                            typedef OrbeReplicaConnection<ProtocolTraits> TSelf;
                            typedef typename Utilities::Connection<ProtocolTraits, TSelf, Wire::OrbeMessageHeaderBytes> TBase;
                            typedef typename TBase::TLogger TLogger;
                            typedef typename ProtocolTraits::TPartitionServer TPartitionServer;
                            typedef typename ProtocolTraits::TNetworkExchange TNetworkExchange;
                            typedef typename ProtocolTraits::TClientGetReplyValueType TClientGetReplyValueType;
                            typedef typename ProtocolTraits::TClientPutReplyValueType TClientPutReplyValueType;
                            typedef typename ProtocolTraits::TReplicationMessageType TReplicationMessageType;
                            typedef typename ProtocolTraits::TBroadcaster TBroadcaster;
                            
                            typedef Wire::OrbeReplicate<ProtocolTraits> OrbeReplicate;
                            typedef Wire::OrbeReplicaIdentityExchange<ProtocolTraits> OrbeReplicaIdentityExchange;
                            
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
                                OrbeReplicate msg(OrbeReplicate::Create(replicateMessage.Key(),
                                                                        replicateMessage.Value(),
                                                                        replicateMessage.Timestamp(),
                                                                        replicateMessage.SourceReplicaId(),
                                                                        replicateMessage.LogicalTimestamp(),
                                                                        replicateMessage.ItemDependencyTimestamp(),
                                                                        replicateMessage.Matrix()));
                                
                                TBase::PostWrite(msg);
                            }
                            
                            void HandleConnectionEstablished()
                            {
                                m_logger.Log("OrbeReplicaConnection::HandleConnectionEstablished\n");
                                
                                const auto replicaId(m_partitionServer.ReplicaId());
                                const auto msg(OrbeReplicaIdentityExchange::Create(replicaId));
                                TBase::PostWrite(msg);
                            }
                            
                            void ProcessBufferedMessage(const Utilities::SubBuffer& messageBytes)
                            {
                                m_logger.Log("OrbeReplicaConnection::ProcessBufferedMessage\n");
                                
                                const auto header = static_cast<Wire::OrbeWireMessages>(messageBytes[0]);
                                
                                switch(header)
                                {
                                    case Wire::ReplicateMsg: {
                                        TBase::template ParseAndHandle<OrbeReplicate>(messageBytes);
                                    } break;
                                    case Wire::ReplicaIdentityExchangeMsg: {
                                        TBase::template ParseAndHandle<OrbeReplicaIdentityExchange>(messageBytes);
                                    } break;
                                    default: {
                                        SR_ASSERT(false, "Replica role does not handle this message type.\n");
                                    }
                                }
                            }
                            
                            void Handle(const OrbeReplicaIdentityExchange& message)
                            {
                                m_broadcaster.InsertLinkToRemoteReplica(message.ReplicaId());
                                m_exchange.RegisterConnectionToReplicaServer(message.ReplicaId(), *this);
                            }
                            
                            void Handle(const OrbeReplicate& message)
                            {
                                TReplicationMessageType replicateMessage(message.Key(),
                                                                         message.Value(),
                                                                         message.LogicalTimestamp(),
                                                                         message.ItemDependencyTimestamp(),
                                                                         message.DependencyMatrix(),
                                                                         message.Timestamp(),
                                                                         message.SourceReplicaId(),
                                                                         m_partitionServer.PartitionId());
                                
                                m_partitionServer.ReceiveReplicateMessage(replicateMessage);
                            }
                            
                        private:
                            OrbeReplicaConnection(boost::asio::io_service& ioService,
                                                  TPartitionServer& partitionServer,
                                                  TBroadcaster& broadcaster,
                                                  TNetworkExchange& exchange,
                                                  TLogger& logger)
                            : Utilities::Connection<ProtocolTraits, TSelf, Wire::OrbeMessageHeaderBytes>(ioService,
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

