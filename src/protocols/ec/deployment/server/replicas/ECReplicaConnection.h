//
//  ECReplicaConnection.h
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
#include "ECWire.h"
#include "Connection.h"
#include "ECReplicate.h"
#include "ECReplicaIdentityExchange.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace EC
        {
            namespace Deployment
            {
                namespace Server
                {
                    namespace Neighbours
                    {
                        template<typename ProtocolTraits>
                        class ECReplicaConnection : public Utilities::Connection<
                        ProtocolTraits,
                        ECReplicaConnection<ProtocolTraits>,
                        Wire::ECMessageHeaderBytes
                        >, private boost::noncopyable
                        {
                            typedef ECReplicaConnection<ProtocolTraits> TSelf;
                            typedef typename Utilities::Connection<ProtocolTraits, TSelf, Wire::ECMessageHeaderBytes> TBase;
                            typedef typename TBase::TLogger TLogger;
                            typedef typename ProtocolTraits::TPartitionServer TPartitionServer;
                            typedef typename ProtocolTraits::TNetworkExchange TNetworkExchange;
                            typedef typename ProtocolTraits::TClientGetReplyValueType TClientGetReplyValueType;
                            typedef typename ProtocolTraits::TClientPutReplyValueType TClientPutReplyValueType;
                            typedef typename ProtocolTraits::TReplicationMessageType TReplicationMessageType;
                            typedef typename ProtocolTraits::TBroadcaster TBroadcaster;
                            
                            typedef Wire::ECReplicate<ProtocolTraits> ECReplicate;
                            typedef Wire::ECReplicaIdentityExchange<ProtocolTraits> ECReplicaIdentityExchange;
                            
                        public:
                            typedef typename TBase::TPtr TPtr;
                            
                            static TPtr Create(boost::asio::io_service& ioService,
                                               TPartitionServer& partitionServer,
                                               TBroadcaster& broadcaster,
                                               TNetworkExchange& exchange,
                                               TLogger& logger)
                            {
                                return TPtr(new TSelf(ioService, partitionServer, broadcaster, exchange, logger));
                            }
                            
                            void SendReplicationMessageToReplica(const TReplicationMessageType& replicateMessage)
                            {
                                ECReplicate msg(ECReplicate::Create(replicateMessage.Key(),
                                                                    replicateMessage.Value(),
                                                                    replicateMessage.Timestamp(),
                                                                    replicateMessage.SourceReplicaId()));
                                
                                TBase::PostWrite(msg);
                            }
                            
                            void HandleConnectionEstablished()
                            {
                                m_logger.Log("ECReplicaConnection::HandleConnectionEstablished\n");
                                
                                const auto replicaId(m_partitionServer.ReplicaId());
                                const auto msg(ECReplicaIdentityExchange::Create(replicaId));
                                TBase::PostWrite(msg);
                            }
                            
                            void ProcessBufferedMessage(const Utilities::SubBuffer& messageBytes)
                            {
                                m_logger.Log("ECReplicaConnection::ProcessBufferedMessage\n");
                                
                                const auto header = static_cast<Wire::ECWireMessages>(messageBytes[0]);
                                
                                switch(header)
                                {
                                    case Wire::ReplicateMsg: {
                                        TBase::template ParseAndHandle<ECReplicate>(messageBytes);
                                    } break;
                                    case Wire::ReplicaIdentityExchangeMsg: {
                                        TBase::template ParseAndHandle<ECReplicaIdentityExchange>(messageBytes);
                                    } break;
                                    default: {
                                        SR_ASSERT(false, "Replica role does not handle this message type.\n");
                                    }
                                }
                            }
                            
                            void Handle(const ECReplicaIdentityExchange& message)
                            {
                                m_broadcaster.InsertLinkToRemoteReplica(message.ReplicaId());
                                m_exchange.RegisterConnectionToReplicaServer(message.ReplicaId(), *this);
                            }
                            
                            void Handle(const ECReplicate& message)
                            {
                                TReplicationMessageType replicateMessage(message.Key(),
                                                                         message.Value(),
                                                                         message.Timestamp(),
                                                                         message.ReplicaId(),
                                                                         m_partitionServer.PartitionId());
                                
                                m_partitionServer.ReceiveReplicateMessage(replicateMessage);
                            }
                            
                        private:
                            ECReplicaConnection(boost::asio::io_service& ioService,
                                                TPartitionServer& partitionServer,
                                                TBroadcaster& broadcaster,
                                                TNetworkExchange& exchange,
                                                TLogger& logger)
                            : Utilities::Connection<ProtocolTraits, TSelf, Wire::ECMessageHeaderBytes>(ioService,
                                                                                                       logger,
                                                                                                       *this)
                            , m_partitionServer(partitionServer)
                            , m_broadcaster(broadcaster)
                            , m_exchange(exchange)
                            , m_logger(logger)
                            {
                                
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

