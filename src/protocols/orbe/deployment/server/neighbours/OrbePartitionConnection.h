//
//  OrbePartitionConnection.h
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
#include "OrbeServerToServerGetRequest.h"
#include "OrbeServerToServerPutRequest.h"
#include "OrbeServerToServerGetResponse.h"
#include "OrbeServerToServerPutResponse.h"
#include "OrbePartitionIdentityExchange.h"
#include "OrbeVersionVectorCheckRequest.h"
#include "OrbeVersionVectorCheckResponse.h"

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
                        class OrbePartitionConnection : public Utilities::Connection<
                        ProtocolTraits,
                        OrbePartitionConnection<ProtocolTraits>,
                        Wire::OrbeMessageHeaderBytes
                        >, private boost::noncopyable
                        {
                            typedef OrbePartitionConnection<ProtocolTraits> TSelf;
                            
                            typedef typename Utilities::Connection<ProtocolTraits, TSelf, Wire::OrbeMessageHeaderBytes> TBase;
                            typedef typename TBase::TLogger TLogger;
                            
                            typedef typename ProtocolTraits::TClient TClient;
                            typedef typename ProtocolTraits::TPartitionServer TPartitionServer;
                            typedef typename ProtocolTraits::TNetworkExchange TNetworkExchange;
                            typedef typename ProtocolTraits::TKeyType TKeyType;
                            typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                            typedef typename ProtocolTraits::TClientGetReplyValueType TClientGetReplyValueType;
                            typedef typename ProtocolTraits::TClientPutReplyValueType TClientPutReplyValueType;
                            typedef typename ProtocolTraits::TBroadcaster TBroadcaster;
                            typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                            typedef typename ProtocolTraits::TDependencyMatrix TDependencyMatrix;
                            typedef typename ProtocolTraits::TVersionVector TVersionVector;
                            
                            typedef Wire::OrbeServerToServerGetRequest<ProtocolTraits> OrbeServerToServerGetRequest;
                            typedef Wire::OrbeServerToServerPutRequest<ProtocolTraits> OrbeServerToServerPutRequest;
                            typedef Wire::OrbeServerToServerGetResponse<ProtocolTraits> OrbeServerToServerGetResponse;
                            typedef Wire::OrbeServerToServerPutResponse<ProtocolTraits> OrbeServerToServerPutResponse;
                            typedef Wire::OrbePartitionIdentityExchange<ProtocolTraits> OrbePartitionIdentityExchange;
                            typedef Wire::OrbeVersionVectorCheckRequest<ProtocolTraits> OrbeVersionVectorCheckRequest;
                            typedef Wire::OrbeVersionVectorCheckResponse<ProtocolTraits> OrbeVersionVectorCheckResponse;
                            
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
                            
                            void HandleConnectionEstablished()
                            {
                                m_logger.Log("OrbePartitionConnection::HandleConnectionEstablished\n");
                                const auto partitionId(m_partitionServer.PartitionId());
                                const auto msg(OrbePartitionIdentityExchange::Create(partitionId));
                                TBase::PostWrite(msg);
                            }
                            
                            void PerformGetForNeighbour(TClient& client, TKeyType key)
                            {
                                m_logger.Log("OrbePartitionConnection::PerformGetForNeighbour\n");
                                const auto clientId = client.ClientId();
                                SR_ASSERT(m_pendingNeighbourOperations[clientId] == nullptr);
                                m_pendingNeighbourOperations[clientId] = &client;
                                const auto msg(OrbeServerToServerGetRequest::Create(key, clientId));
                                TBase::PostWrite(msg);
                            }
                            
                            void PerformPutForNeighbour(TClient& client,
                                                        TKeyType key,
                                                        TClientInputValueType value,
                                                        TDependencyMatrix& dependencyMatrix)
                            {
                                m_logger.Log("OrbePartitionConnection::PerformPutForNeighbour\n");
                                const auto clientId = client.ClientId();
                                SR_ASSERT(m_pendingNeighbourOperations[clientId] == nullptr);
                                m_pendingNeighbourOperations[clientId] = &client;
                                const auto msg(OrbeServerToServerPutRequest::Create(key, clientId, value, dependencyMatrix));
                                TBase::PostWrite(msg);
                            }
                            
                            void RequestIsNeighbourVersionVectorGreaterThanOrEqualToReplicationDependencyVector(PartitionIdType senderPartitionId,
                                                                                                                ReplicaIdType sourceReplicaId,
                                                                                                                const TVersionVector& dependencyVersionVector)
                            {
                                m_logger.Log("OrbePartitionConnection::RequestIsNeighbourVersionVectorGreaterThanOrEqualToReplicationDependencyVector\n");
                                const auto msg(OrbeVersionVectorCheckRequest::Create(senderPartitionId,
                                                                                     sourceReplicaId,
                                                                                     dependencyVersionVector));
                                TBase::PostWrite(msg);
                            }
                            
                            void SendUnblockingVersionVectorDependencyCheckResponse(ReplicaIdType sourceReplicaId)
                            {
                                m_logger.Log("OrbePartitionConnection::SendUnblockingVersionVectorDependencyCheckResponse\n");
                                const auto msg(OrbeVersionVectorCheckResponse::Create(sourceReplicaId));
                                TBase::PostWrite(msg);
                            }
                            
                            void ProcessBufferedMessage(const Utilities::SubBuffer& messageBytes)
                            {
                                m_logger.Log("OrbePartitionConnection::ProcessBufferedMessage\n");
                                
                                const auto header = static_cast<Wire::OrbeWireMessages>(messageBytes[0]);
                                
                                switch(header)
                                {
                                    case Wire::ServerToServerGetRequestMsg: {
                                        TBase::template ParseAndHandle<OrbeServerToServerGetRequest>(messageBytes);
                                    } break;
                                    case Wire::ServerToServerPutRequestMsg: {
                                        TBase::template ParseAndHandle<OrbeServerToServerPutRequest>(messageBytes);
                                    } break;
                                    case Wire::ServerToServerGetResponseMsg: {
                                        TBase::template ParseAndHandle<OrbeServerToServerGetResponse>(messageBytes);
                                    } break;
                                    case Wire::ServerToServerPutResponseMsg: {
                                        TBase::template ParseAndHandle<OrbeServerToServerPutResponse>(messageBytes);
                                    } break;
                                    case Wire::PartitionIdentityExchangeMsg: {
                                        TBase::template ParseAndHandle<OrbePartitionIdentityExchange>(messageBytes);
                                    } break;
                                    case Wire::VersionVectorCheckRequestMsg: {
                                        TBase::template ParseAndHandle<OrbeVersionVectorCheckRequest>(messageBytes);
                                    } break;
                                    case Wire::VersionVectorCheckResponseMsg: {
                                        TBase::template ParseAndHandle<OrbeVersionVectorCheckResponse>(messageBytes);
                                    } break;
                                    default: {
                                        SR_ASSERT(false, "Partition role does not handle this message type.\n");
                                    }
                                }
                            }
                            
                            void Handle(const OrbeServerToServerGetRequest& message)
                            {
                                m_partitionServer.Get(message.Key(),
                                                      *this,
                                                      message);
                            }
                            
                            void Handle(const OrbeServerToServerPutRequest& message)
                            {
                                m_partitionServer.Put(message.Key(),
                                                      message.Value(),
                                                      *this,
                                                      message.DependencyMatrix(),
                                                      message);
                            }
                            
                            void Handle(const OrbeServerToServerGetResponse& message)
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
                                                                   message.SourceReplicaId(),
                                                                   message.LogicalTimestamp());
                                    
                                    client.HandleGetCompleteItemFound(reply);
                                }
                                else
                                {
                                    client.HandleGetCompleteItemNotFound();
                                }
                            }
                            
                            void Handle(const OrbeServerToServerPutResponse& message)
                            {
                                // send to client
                                const auto clientId = message.ClientId();
                                const auto it = m_pendingNeighbourOperations.find(clientId);
                                SR_ASSERT(it != m_pendingNeighbourOperations.end());
                                TClient& client = *(it->second);
                                SR_ASSERT(client.IsBusy());
                                m_pendingNeighbourOperations.erase(it);
                                
                                TClientPutReplyValueType reply(message.Timestamp(),
                                                               message.SourceReplicaId(),
                                                               message.LogicalTimestamp());
                                client.HandlePutComplete(reply);
                            }
                            
                            void Handle(const OrbePartitionIdentityExchange& message)
                            {
                                m_broadcaster.InsertLinkToLocalPartitionNeighbour(message.PartitionId());
                                m_exchange.RegisterConnectionToNeighbourServer(message.PartitionId(), *this);
                            }
                            
                            void Handle(const OrbeVersionVectorCheckRequest& message)
                            {
                                m_partitionServer.ReceiveNeighbourVersionVectorDependencyCheckRequest(message.SenderPartitionId(),
                                                                                                      message.DependencyVersionVector(),
                                                                                                      message.SourceReplicaId());
                            }
                            
                            void Handle(const OrbeVersionVectorCheckResponse& message)
                            {
                                m_partitionServer.ReceiveNeighbourVersionVectorDependencyCheckResponse(message.SourceReplicaId());
                            }
                            
                            void HandleGetCompleteItemFound(const TClientGetReplyValueType& getReply,
                                                            const OrbeServerToServerGetRequest& message)
                            {
                                const auto msg(OrbeServerToServerGetResponse::Create(Utilities::Byte(1),
                                                                                     message.Key(),
                                                                                     getReply.Value(),
                                                                                     getReply.Timestamp(),
                                                                                     message.ClientId(),
                                                                                     getReply.SourceReplicaId(),
                                                                                     getReply.LogicalTimestamp()));
                                
                                TBase::PostWrite(msg);
                            }
                            
                            void HandleGetCompleteItemNotFound(const OrbeServerToServerGetRequest& message)
                            {
                                typedef Utilities::TTimestamp TTimestamp;
                                
                                const auto msg(OrbeServerToServerGetResponse::Create(Utilities::Byte(0),
                                                                                     message.Key(),
                                                                                     0,
                                                                                     TTimestamp(),
                                                                                     message.ClientId(),
                                                                                     ReplicaIdType(),
                                                                                     TLogicalTimestamp()));
                                
                                TBase::PostWrite(msg);
                            }
                            
                            void HandlePutComplete(const TClientPutReplyValueType& putReply,
                                                   const OrbeServerToServerPutRequest& message)
                            {
                                const auto msg(OrbeServerToServerPutResponse::Create(putReply.Timestamp(),
                                                                                     message.ClientId(),
                                                                                     putReply.SourceReplicaId(),
                                                                                     putReply.LogicalTimestamp()));
                                
                                TBase::PostWrite(msg);
                            }
                            
                            
                        private:
                            OrbePartitionConnection(boost::asio::io_service& ioService,
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

