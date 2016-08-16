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
#include "ECClientToServerGetRequest.h"
#include "ECClientToServerPutRequest.h"
#include "ECServerToClientGetResponse.h"
#include "ECServerToClientPutResponse.h"
#include "Timing.h"

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
                    namespace Client
                    {
                        template<typename ProtocolTraits>
                        class ECClientConnection : public Utilities::Connection<
                        ProtocolTraits,
                        ECClientConnection<ProtocolTraits>,
                        Wire::ECMessageHeaderBytes
                        >, private boost::noncopyable
                        {
                            typedef ECClientConnection<ProtocolTraits> TSelf;
                            typedef typename Utilities::Connection<ProtocolTraits, TSelf, Wire::ECMessageHeaderBytes> TBase;
                            typedef typename TBase::TLogger TLogger;
                            typedef typename ProtocolTraits::TMetrics TMetrics;
                            typedef typename ProtocolTraits::TPartitionServer TPartitionServer;
                            typedef typename ProtocolTraits::TNetworkExchange TNetworkExchange;
                            typedef typename ProtocolTraits::TClientGetReplyValueType TClientGetReplyValueType;
                            typedef typename ProtocolTraits::TClientPutReplyValueType TClientPutReplyValueType;
                            
                            typedef Wire::ECClientToServerGetRequest<ProtocolTraits> ECClientToServerGetRequest;
                            typedef Wire::ECClientToServerPutRequest<ProtocolTraits> ECClientToServerPutRequest;
                            typedef Wire::ECServerToClientGetResponse<ProtocolTraits> ECServerToClientGetResponse;
                            typedef Wire::ECServerToClientPutResponse<ProtocolTraits> ECServerToClientPutResponse;
                            
                            typedef typename ProtocolTraits::TClient TClient;
                            typedef typename ProtocolTraits::TKeyToPartitionMapper TKeyToPartitionMapper;
                            typedef std::unique_ptr<TClient> TClientPtr;
                            
                        public:
                            typedef typename TBase::TPtr TPtr;
                            
                            static TPtr Create(ClientIdType clientId,
                                               TKeyToPartitionMapper& keyToPartitionMapper,
                                               boost::asio::io_service& ioService,
                                               TPartitionServer& partitionServer,
                                               TNetworkExchange& networkExchange,
                                               TMetrics& metrics,
                                               TLogger& logger)
                            {
                                return TPtr(new TSelf(clientId, keyToPartitionMapper, ioService, partitionServer, networkExchange, metrics, logger));
                            }
                            
                            void HandleConnectionEstablished()
                            {
                                m_logger.Log("ECClientConnection::HandleConnectionEstablished\n");
                            }
                            
                            void ProcessBufferedMessage(const Utilities::SubBuffer& messageBytes)
                            {
                                m_logger.Log("ECClientConnection::ProcessBufferedMessage\n");
                                
                                const auto header = static_cast<Wire::ECWireMessages>(messageBytes[0]);
                                
                                switch(header)
                                {
                                    case Wire::ClientToServerGetRequestMsg: {
                                        TBase::template ParseAndHandle<ECClientToServerGetRequest>(messageBytes);
                                    } break;
                                    case Wire::ClientToServerPutRequestMsg: {
                                        TBase::template ParseAndHandle<ECClientToServerPutRequest>(messageBytes);
                                    } break;
                                    default: {
                                        SR_ASSERT(false, "Client role does not handle this message type.\n");
                                    }
                                }
                            }
                            
                            void Handle(const ECClientToServerGetRequest& message)
                            {
                                Client().IssueGet(message.Key());
                            }
                            
                            void Handle(const ECClientToServerPutRequest& message)
                            {
                                Client().IssuePut(message.Key(), message.Value());
                            }
                            
                            void HandleGetCompleteItemFound(const TClientGetReplyValueType& getReply)
                            {
                                m_logger.Log("ECClientConnection::HandleGetCompleteItemFound: %s, %llu, %d\n",
                                             getReply.Value().c_str(),
                                             getReply.Timestamp(),
                                             getReply.SourceReplicaId());
                                
                                const auto msg(ECServerToClientGetResponse::CreateFound(getReply.Value()));
                                TBase::PostWrite(msg);
                            }
                            
                            void HandleGetCompleteItemNotFound()
                            {
                                m_logger.Log("ECClientConnection::HandleGetCompleteItemNotFound\n");
                                const auto msg(ECServerToClientGetResponse::CreateNotFound());
                                TBase::PostWrite(msg);
                            }
                            
                            void HandlePutComplete(const TClientPutReplyValueType& putReply)
                            {
                                m_logger.Log("ECClientConnection::HandlePutComplete: %llu, %d\n",
                                             putReply.Timestamp(),
                                             putReply.SourceReplicaId());
                                
                                const auto msg(ECServerToClientPutResponse::Create());
                                TBase::PostWrite(msg);
                            }
                            
                        private:
                            ECClientConnection(ClientIdType clientId,
                                               TKeyToPartitionMapper& keyToPartitionMapper,
                                               boost::asio::io_service& ioService,
                                               TPartitionServer& partitionServer,
                                               TNetworkExchange& networkExchange,
                                               TMetrics& metrics,
                                               TLogger& logger)
                            : Utilities::Connection<ProtocolTraits, TSelf, Wire::ECMessageHeaderBytes>(ioService,
                                                                                                       logger,
                                                                                                       *this)
                            , m_pClient(new TClient(clientId,
                                                    keyToPartitionMapper,
                                                    networkExchange,
                                                    partitionServer,
                                                    *this,
                                                    metrics,
                                                    logger))
                            , m_partitionServer(partitionServer)
                            , m_exchange(networkExchange)
                            , m_logger(logger)
                            {
                                
                            }
                            
                            TClient& Client() const { return *m_pClient; }
                            
                            TClientPtr m_pClient;
                            TPartitionServer& m_partitionServer;
                            TNetworkExchange& m_exchange;
                            TLogger& m_logger;
                        };
                    }
                }
            }
        }
    }
}

