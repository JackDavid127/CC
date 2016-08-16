//
//  OrbeDelayedInstallPartitionConnection.h
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
#include "OrbeDelayedInstallServerToServerGetRequest.h"
#include "OrbeDelayedInstallServerToServerPutRequest.h"
#include "OrbeDelayedInstallServerToServerGetResponse.h"
#include "OrbeDelayedInstallServerToServerPutResponse.h"
#include "OrbeDelayedInstallPartitionIdentityExchange.h"
#include "OrbeDelayedInstallDependencyTimestampCheckRequest.h"
#include "OrbeDelayedInstallDependencyTimestampCheckResponse.h"

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
                        class OrbeDelayedInstallPartitionConnection : public Utilities::Connection<
                        ProtocolTraits,
                        OrbeDelayedInstallPartitionConnection<ProtocolTraits>,
                        Wire::OrbeDelayedInstallMessageHeaderBytes
                        >, private boost::noncopyable
                        {
                            typedef OrbeDelayedInstallPartitionConnection<ProtocolTraits> TSelf;
                            
                            typedef typename Utilities::Connection<ProtocolTraits, TSelf, Wire::OrbeDelayedInstallMessageHeaderBytes> TBase;
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
                            typedef typename ProtocolTraits::TClientDependencyTimestamp TClientDependencyTimestamp;
                            typedef typename ProtocolTraits::TClientDependencyPartition TClientDependencyPartition;
                            typedef typename ProtocolTraits::TVersionVector TVersionVector;
                            
                            typedef Wire::OrbeDelayedInstallServerToServerGetRequest<ProtocolTraits> OrbeDelayedInstallServerToServerGetRequest;
                            typedef Wire::OrbeDelayedInstallServerToServerPutRequest<ProtocolTraits> OrbeDelayedInstallServerToServerPutRequest;
                            typedef Wire::OrbeDelayedInstallServerToServerGetResponse<ProtocolTraits> OrbeDelayedInstallServerToServerGetResponse;
                            typedef Wire::OrbeDelayedInstallServerToServerPutResponse<ProtocolTraits> OrbeDelayedInstallServerToServerPutResponse;
                            typedef Wire::OrbeDelayedInstallPartitionIdentityExchange<ProtocolTraits> OrbeDelayedInstallPartitionIdentityExchange;
                            typedef Wire::OrbeDelayedInstallDependencyTimestampCheckRequest<ProtocolTraits> OrbeDelayedInstallDependencyTimestampCheckRequest;
                            typedef Wire::OrbeDelayedInstallDependencyTimestampCheckResponse<ProtocolTraits> OrbeDelayedInstallDependencyTimestampCheckResponse;
                            
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
                                m_logger.Log("OrbeDelayedInstallPartitionConnection::HandleConnectionEstablished\n");
                                const auto partitionId(m_partitionServer.PartitionId());
                                const auto msg(OrbeDelayedInstallPartitionIdentityExchange::Create(partitionId));
                                TBase::PostWrite(msg);
                            }
                            
                            void PerformGetForNeighbour(TClient& client, TKeyType key)
                            {
                                m_logger.Log("OrbeDelayedInstallPartitionConnection::PerformGetForNeighbour\n");
                                const auto clientId = client.ClientId();
                                SR_ASSERT(m_pendingNeighbourOperations[clientId] == nullptr);
                                m_pendingNeighbourOperations[clientId] = &client;
                                const auto msg(OrbeDelayedInstallServerToServerGetRequest::Create(key, clientId));
                                TBase::PostWrite(msg);
                            }
                            
                            void PerformPutForNeighbour(TClient& client,
                                                        TKeyType key,
                                                        TClientInputValueType value,
                                                        TClientDependencyTimestamp clientDependencyTimestamp,
                                                        TClientDependencyPartition clientDependencyPartition)
                            {
                                m_logger.Log("OrbeDelayedInstallPartitionConnection::PerformPutForNeighbour\n");
                                const auto clientId = client.ClientId();
                                SR_ASSERT(m_pendingNeighbourOperations[clientId] == nullptr);
                                m_pendingNeighbourOperations[clientId] = &client;
                                const auto msg(OrbeDelayedInstallServerToServerPutRequest::Create(key,
                                                                                                  clientId,
                                                                                                  value,
                                                                                                  clientDependencyTimestamp,
                                                                                                  clientDependencyPartition));
                                TBase::PostWrite(msg);
                            }
                            
                            void RequestIsNeighbourTimestampGreaterThanOrEqualToReplicationDependencyTimestamp(PartitionIdType senderPartitionId,
                                                                                                               ReplicaIdType sourceReplicaId,
                                                                                                               const TClientDependencyTimestamp& clientDependencyTimestamp)
                            {
                                m_logger.Log("exchange::RequestIsNeighbourDependencyTimestampGreaterThanOrEqualToReplicaDependencyVector\n");
                                const auto msg(OrbeDelayedInstallDependencyTimestampCheckRequest::CreateLocal(senderPartitionId,
                                                                                                              sourceReplicaId,
                                                                                                              clientDependencyTimestamp));
                                TBase::PostWrite(msg);
                            }
                            
                            void SendUnblockingDependencyTimestampCheckResponse(ReplicaIdType sourceReplicaId)
                            {
                                m_logger.Log("exchange::SendUnblockingDependencyTimestampCheckResponse\n");
                                const auto msg(OrbeDelayedInstallDependencyTimestampCheckResponse::CreateLocal(sourceReplicaId));
                                TBase::PostWrite(msg);
                            }
                            
                            void RequestIsNeighbourGlobalTimestampNotLessThanDependencyGlobalTimestamp(PartitionIdType senderPartitionId,
                                                                                                       ReplicaIdType sourceReplicaId,
                                                                                                       const TClientDependencyTimestamp& clientDependencyTimestamp)
                            {
                                m_logger.Log("exchange::RequestIsNeighbourDependencyTimestampGreaterThanOrEqualToReplicaDependencyVector\n");
                                const auto msg(OrbeDelayedInstallDependencyTimestampCheckRequest::CreateGlobal(senderPartitionId,
                                                                                                               sourceReplicaId,
                                                                                                               clientDependencyTimestamp));
                                TBase::PostWrite(msg);
                            }
                            
                            void SendUnblockingGlobalDependencyTimestampCheckResponse(ReplicaIdType sourceReplicaId)
                            {
                                m_logger.Log("exchange::SendUnblockingGlobalDependencyTimestampCheckResponse\n");
                                const auto msg(OrbeDelayedInstallDependencyTimestampCheckResponse::CreateGlobal(sourceReplicaId));
                                TBase::PostWrite(msg);
                            }
                            
                            void ProcessBufferedMessage(const Utilities::SubBuffer& messageBytes)
                            {
                                m_logger.Log("OrbeDelayedInstallPartitionConnection::ProcessBufferedMessage\n");
                                
                                const auto header = static_cast<Wire::OrbeDelayedInstallWireMessages>(messageBytes[0]);
                                
                                switch(header)
                                {
                                    case Wire::ServerToServerGetRequestMsg: {
                                        TBase::template ParseAndHandle<OrbeDelayedInstallServerToServerGetRequest>(messageBytes);
                                    } break;
                                    case Wire::ServerToServerPutRequestMsg: {
                                        TBase::template ParseAndHandle<OrbeDelayedInstallServerToServerPutRequest>(messageBytes);
                                    } break;
                                    case Wire::ServerToServerGetResponseMsg: {
                                        TBase::template ParseAndHandle<OrbeDelayedInstallServerToServerGetResponse>(messageBytes);
                                    } break;
                                    case Wire::ServerToServerPutResponseMsg: {
                                        TBase::template ParseAndHandle<OrbeDelayedInstallServerToServerPutResponse>(messageBytes);
                                    } break;
                                    case Wire::PartitionIdentityExchangeMsg: {
                                        TBase::template ParseAndHandle<OrbeDelayedInstallPartitionIdentityExchange>(messageBytes);
                                    } break;
                                    case Wire::DependencyTimestampCheckRequestMsg: {
                                        TBase::template ParseAndHandle<OrbeDelayedInstallDependencyTimestampCheckRequest>(messageBytes);
                                    } break;
                                    case Wire::DependencyTimestampCheckResponseMsg: {
                                        TBase::template ParseAndHandle<OrbeDelayedInstallDependencyTimestampCheckResponse>(messageBytes);
                                    } break;
                                    default: {
                                        SR_ASSERT(false, "Partition role does not handle this message type.\n");
                                    }
                                }
                            }
                            
                            void Handle(const OrbeDelayedInstallServerToServerGetRequest& message)
                            {
                                m_partitionServer.Get(message.Key(),
                                                      message.ClientId(),
                                                      *this,
                                                      message);
                            }
                            
                            void Handle(const OrbeDelayedInstallServerToServerPutRequest& message)
                            {
                                m_partitionServer.Put(message.Key(),
                                                      message.Value(),
                                                      message.ClientId(),
                                                      message.ClientDependencyTimestamp(),
                                                      message.ClientDependencyPartition(),
                                                      *this,
                                                      message);
                            }
                            
                            void Handle(const OrbeDelayedInstallServerToServerGetResponse& message)
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
                            
                            void Handle(const OrbeDelayedInstallServerToServerPutResponse& message)
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
                            
                            void Handle(const OrbeDelayedInstallPartitionIdentityExchange& message)
                            {
                                m_broadcaster.InsertLinkToLocalPartitionNeighbour(message.PartitionId());
                                m_exchange.RegisterConnectionToNeighbourServer(message.PartitionId(), *this);
                            }
                            
                            void Handle(const OrbeDelayedInstallDependencyTimestampCheckRequest& message)
                            {
                                if(message.IsGlobalVectorCheck())
                                {
                                    m_partitionServer.ReceiveNeighbourGlobalTimestampDependencyCheckRequest(message.SenderPartitionId(),
                                                                                                            message.ClientDependencyTimestamp(),
                                                                                                            message.SourceReplicaId());
                                }
                                else
                                {
                                    m_partitionServer.ReceiveNeighbourTimestampDependencyCheckRequest(message.SenderPartitionId(),
                                                                                                      message.ClientDependencyTimestamp(),
                                                                                                      message.SourceReplicaId());
                                }
                            }
                            
                            void Handle(const OrbeDelayedInstallDependencyTimestampCheckResponse& message)
                            {
                                if(message.IsGlobalVectorCheck())
                                {
                                    m_partitionServer.ReceiveNeighbourGlobalTimestampDependencyCheckResponse(message.SourceReplicaId());
                                }
                                else
                                {
                                    m_partitionServer.ReceiveNeighbourTimestampDependencyCheckResponse(message.SourceReplicaId());
                                }
                            }
                            
                            void HandleGetCompleteItemFound(const TClientGetReplyValueType& getReply,
                                                            const OrbeDelayedInstallServerToServerGetRequest& message)
                            {
                                const auto msg(OrbeDelayedInstallServerToServerGetResponse::Create(Utilities::Byte(1),
                                                                                                   message.Key(),
                                                                                                   getReply.Value(),
                                                                                                   getReply.Timestamp(),
                                                                                                   message.ClientId(),
                                                                                                   getReply.SourceReplicaId(),
                                                                                                   getReply.LogicalTimestamp()));
                                
                                TBase::PostWrite(msg);
                            }
                            
                            void HandleGetCompleteItemNotFound(const OrbeDelayedInstallServerToServerGetRequest& message)
                            {
                                typedef Utilities::TTimestamp TTimestamp;
                                
                                const auto msg(OrbeDelayedInstallServerToServerGetResponse::Create(Utilities::Byte(0),
                                                                                                   message.Key(),
                                                                                                   0,
                                                                                                   TTimestamp(),
                                                                                                   message.ClientId(),
                                                                                                   ReplicaIdType(),
                                                                                                   TLogicalTimestamp()));
                                
                                TBase::PostWrite(msg);
                            }
                            
                            void HandlePutComplete(const TClientPutReplyValueType& putReply,
                                                   const OrbeDelayedInstallServerToServerPutRequest& message)
                            {
                                const auto msg(OrbeDelayedInstallServerToServerPutResponse::Create(putReply.Timestamp(),
                                                                                                   message.ClientId(),
                                                                                                   putReply.SourceReplicaId(),
                                                                                                   putReply.LogicalTimestamp()));
                                
                                TBase::PostWrite(msg);
                            }
                            
                            
                        private:
                            OrbeDelayedInstallPartitionConnection(boost::asio::io_service& ioService,
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

