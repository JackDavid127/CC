//
//  ECPartitionConnection.h
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
#include "ECServerToServerGetRequest.h"
#include "ECServerToServerPutRequest.h"
#include "ECServerToServerGetResponse.h"
#include "ECServerToServerPutResponse.h"
#include "ECPartitionIdentityExchange.h"

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
                        class ECPartitionConnection : public Utilities::Connection<
                        ProtocolTraits,
                        ECPartitionConnection<ProtocolTraits>,
                        Wire::ECMessageHeaderBytes
                        >, private boost::noncopyable
                        {
                            typedef ECPartitionConnection<ProtocolTraits> TSelf;
                            
                            typedef typename Utilities::Connection<ProtocolTraits, TSelf, Wire::ECMessageHeaderBytes> TBase;
                            typedef typename TBase::TLogger TLogger;
                            
                            typedef typename ProtocolTraits::TClient TClient;
                            typedef typename ProtocolTraits::TPartitionServer TPartitionServer;
                            typedef typename ProtocolTraits::TNetworkExchange TNetworkExchange;
                            typedef typename ProtocolTraits::TKeyType TKeyType;
                            typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                            typedef typename ProtocolTraits::TClientGetReplyValueType TClientGetReplyValueType;
                            typedef typename ProtocolTraits::TClientPutReplyValueType TClientPutReplyValueType;
                            typedef typename ProtocolTraits::TBroadcaster TBroadcaster;
                            
                            typedef Wire::ECServerToServerGetRequest<ProtocolTraits> ECServerToServerGetRequest;
                            typedef Wire::ECServerToServerPutRequest<ProtocolTraits> ECServerToServerPutRequest;
                            typedef Wire::ECServerToServerGetResponse<ProtocolTraits> ECServerToServerGetResponse;
                            typedef Wire::ECServerToServerPutResponse<ProtocolTraits> ECServerToServerPutResponse;
                            typedef Wire::ECPartitionIdentityExchange<ProtocolTraits> ECPartitionIdentityExchange;
                            
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
                            
                            void HandleConnectionEstablished()
                            {
                                m_logger.Log("ECPartitionConnection::HandleConnectionEstablished\n");
                                
                                const auto partitionId(m_partitionServer.PartitionId());
                                const auto msg(ECPartitionIdentityExchange::Create(partitionId));
                                TBase::PostWrite(msg);
                            }
                            
                            void PerformGetForNeighbour(ClientIdType clientId, TClient& client, TKeyType key)
                            {
                                m_logger.Log("ECPartitionConnection::PerformGetForNeighbour\n");
                                SR_ASSERT(m_pendingNeighbourOperations[clientId] == nullptr, "Client %d has pending transaction\n", clientId);
                                m_pendingNeighbourOperations[clientId] = &client;
                                const auto msg(ECServerToServerGetRequest::Create(key, clientId));
                                TBase::PostWrite(msg);
                            }
                            
                            void PerformPutForNeighbour(ClientIdType clientId, TClient& client, TKeyType key, TClientInputValueType value)
                            {
                                m_logger.Log("ECPartitionConnection::PerformPutForNeighbour\n");
                                SR_ASSERT(m_pendingNeighbourOperations[clientId] == nullptr, "Client %d has pending transaction\n", clientId);
                                m_pendingNeighbourOperations[clientId] = &client;
                                const auto msg(ECServerToServerPutRequest::Create(key, clientId, value));
                                TBase::PostWrite(msg);
                            }
                            
                            void ProcessBufferedMessage(const Utilities::SubBuffer& messageBytes)
                            {
                                m_logger.Log("ECPartitionConnection::ProcessBufferedMessage\n");
                                
                                const auto header = static_cast<Wire::ECWireMessages>(messageBytes[0]);
                                
                                switch(header)
                                {
                                    case Wire::ServerToServerGetRequestMsg: {
                                        TBase::template ParseAndHandle<ECServerToServerGetRequest>(messageBytes);
                                    } break;
                                    case Wire::ServerToServerPutRequestMsg: {
                                        TBase::template ParseAndHandle<ECServerToServerPutRequest>(messageBytes);
                                    } break;
                                    case Wire::ServerToServerGetResponseMsg: {
                                        TBase::template ParseAndHandle<ECServerToServerGetResponse>(messageBytes);
                                    } break;
                                    case Wire::ServerToServerPutResponseMsg: {
                                        TBase::template ParseAndHandle<ECServerToServerPutResponse>(messageBytes);
                                    } break;
                                    case Wire::PartitionIdentityExchangeMsg: {
                                        TBase::template ParseAndHandle<ECPartitionIdentityExchange>(messageBytes);
                                    } break;
                                    default: {
                                        SR_ASSERT(false, "Partition role does not handle this message type.\n");
                                    }
                                }
                            }
                            
                            void Handle(const ECServerToServerGetRequest& message)
                            {
                                m_partitionServer.Get(message.Key(), *this, message);
                            }
                            
                            void Handle(const ECServerToServerPutRequest& message)
                            {
                                m_partitionServer.Put(message.Key(), message.Value(), *this, message);
                            }
                            
                            void Handle(const ECServerToServerGetResponse& message)
                            {
                                // send to client
                                const auto clientId = message.ClientId();
                                const auto it = m_pendingNeighbourOperations.find(clientId);
                                SR_ASSERT(it != m_pendingNeighbourOperations.end());
                                TClient& client = *(it->second);
                                SR_ASSERT(client.IsBusy());
                                m_pendingNeighbourOperations.erase(it);
                                
                                if(message.FoundValue())
                                {
                                    TClientGetReplyValueType reply(message.Value(),
                                                                   message.Timestamp(),
                                                                   m_partitionServer.ReplicaId());
                                    
                                    client.HandleGetCompleteItemFound(reply);
                                }
                                else
                                {
                                    client.HandleGetCompleteItemNotFound();
                                }
                            }
                            
                            void Handle(const ECServerToServerPutResponse& message)
                            {
                                // send to client
                                const auto clientId = message.ClientId();
                                const auto it = m_pendingNeighbourOperations.find(clientId);
                                SR_ASSERT(it != m_pendingNeighbourOperations.end());
                                TClient& client = *(it->second);
                                SR_ASSERT(client.IsBusy());
                                m_pendingNeighbourOperations.erase(it);
                                
                                TClientPutReplyValueType reply(message.Timestamp(),
                                                               m_partitionServer.ReplicaId());
                                client.HandlePutComplete(reply);
                            }
                            
                            void Handle(const ECPartitionIdentityExchange& message)
                            {
                                m_broadcaster.InsertLinkToLocalPartitionNeighbour(message.PartitionId());
                                m_exchange.RegisterConnectionToNeighbourServer(message.PartitionId(), *this);
                            }
                            
                            void HandleGetCompleteItemFound(const TClientGetReplyValueType& getReply,
                                                            const ECServerToServerGetRequest& message)
                            {
                                const auto msg(ECServerToServerGetResponse::Create(Utilities::Byte(1),
                                                                                   message.Key(),
                                                                                   getReply.Value(),
                                                                                   getReply.Timestamp(),
                                                                                   message.ClientId()));
                                
                                TBase::PostWrite(msg);
                            }
                            
                            void HandleGetCompleteItemNotFound(const ECServerToServerGetRequest& message)
                            {
                                typedef Utilities::TTimestamp TTimestamp;
                                
                                const auto msg(ECServerToServerGetResponse::Create(Utilities::Byte(0),
                                                                                   message.Key(),
                                                                                   "",
                                                                                   TTimestamp(),
                                                                                   message.ClientId()));
                                
                                TBase::PostWrite(msg);
                            }
                            
                            void HandlePutComplete(const TClientPutReplyValueType& putReply,
                                                   const ECServerToServerPutRequest& message)
                            {
                                const auto msg(ECServerToServerPutResponse::Create(putReply.Timestamp(),
                                                                                   message.ClientId()));
                                
                                TBase::PostWrite(msg);
                            }
                            
                            
                        private:
                            ECPartitionConnection(boost::asio::io_service& ioService,
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
                            , m_pendingNeighbourOperations()
                            {
                                
                            }
                            
                            TPartitionServer& m_partitionServer;
                            TBroadcaster& m_broadcaster;
                            TNetworkExchange& m_exchange;
                            TLogger& m_logger;
                            std::map<ClientIdType, TClient*> m_pendingNeighbourOperations;
                        };
                    }
                }
            }
        }
    }
}

