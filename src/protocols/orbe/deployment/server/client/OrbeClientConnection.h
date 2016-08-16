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
#include "OrbeClientToServerGetRequest.h"
#include "OrbeClientToServerPutRequest.h"
#include "OrbeServerToClientGetResponse.h"
#include "OrbeServerToClientPutResponse.h"
#include "Timing.h"

#define OrbePartitionConnection_DEBUG_MESSAGES_INCLUDED

#ifdef OrbePartitionConnection_DEBUG_MESSAGES_INCLUDED
#include "OrbeManualClientStatus.h"
#endif


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
                    namespace Client
                    {
                        template<typename ProtocolTraits>
                        class OrbeClientConnection : public Utilities::Connection<
                        ProtocolTraits,
                        OrbeClientConnection<ProtocolTraits>,
                        Wire::OrbeMessageHeaderBytes
                        >, private boost::noncopyable
                        {
                            typedef OrbeClientConnection<ProtocolTraits> TSelf;
                            typedef typename Utilities::Connection<ProtocolTraits, TSelf, Wire::OrbeMessageHeaderBytes> TBase;
                            typedef typename TBase::TLogger TLogger;
                            typedef typename ProtocolTraits::TMetrics TMetrics;
                            typedef typename ProtocolTraits::TPartitionServer TPartitionServer;
                            typedef typename ProtocolTraits::TNetworkExchange TNetworkExchange;
                            typedef typename ProtocolTraits::TClientGetReplyValueType TClientGetReplyValueType;
                            typedef typename ProtocolTraits::TClientPutReplyValueType TClientPutReplyValueType;
                            
                            typedef Wire::OrbeClientToServerGetRequest<ProtocolTraits> OrbeClientToServerGetRequest;
                            typedef Wire::OrbeClientToServerPutRequest<ProtocolTraits> OrbeClientToServerPutRequest;
                            typedef Wire::OrbeServerToClientGetResponse<ProtocolTraits> OrbeServerToClientGetResponse;
                            typedef Wire::OrbeServerToClientPutResponse<ProtocolTraits> OrbeServerToClientPutResponse;
                            
#ifdef OrbePartitionConnection_DEBUG_MESSAGES_INCLUDED
                            typedef Wire::OrbeManualClientStatus<ProtocolTraits> OrbeManualClientStatus;
#endif
                            
                            typedef typename ProtocolTraits::TClient TClient;
                            typedef typename ProtocolTraits::TKeyToPartitionMapper TKeyToPartitionMapper;
                            typedef std::shared_ptr<TClient> TClientPtr;
                            
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
                                auto pResult = TPtr(new TSelf(ioService, partitionServer, networkExchange, metrics, logger));
                                
                                pResult->SetupSharedPtrToSelf();
                                
                                auto pClient = TClientPtr(new TClient(clientId,
                                                                      keyToPartitionMapper,
                                                                      networkExchange,
                                                                      partitionServer,
                                                                      pResult->SharedPtrToSelf(),
                                                                      metrics,
                                                                      logger));
                                
                                pResult->AttachClient(pClient);
                                
                                return pResult;
                            }
                            
                            void HandleConnectionEstablished()
                            {
                                //m_logger.Log("OrbeClientConnection::HandleConnectionEstablished\n");
                            }
                            
                            void ProcessBufferedMessage(const Utilities::SubBuffer& messageBytes)
                            {
                                //m_logger.Log("OrbeClientConnection::ProcessBufferedMessage\n");
                                
                                const auto header = static_cast<Wire::OrbeWireMessages>(messageBytes[0]);
                                
                                switch(header)
                                {
                                    case Wire::ClientToServerGetRequestMsg: {
                                        TBase::template ParseAndHandle<OrbeClientToServerGetRequest>(messageBytes);
                                    } break;
                                    case Wire::ClientToServerPutRequestMsg: {
                                        TBase::template ParseAndHandle<OrbeClientToServerPutRequest>(messageBytes);
                                    } break;
#ifdef OrbePartitionConnection_DEBUG_MESSAGES_INCLUDED
                                    case Wire::ManualClientStatusMsg: {
                                        TBase::template ParseAndHandle<OrbeManualClientStatus>(messageBytes);
                                    } break;
#endif
                                    default: {
                                        SR_ASSERT(false, "Client role does not handle this message type.\n");
                                    }
                                }
                            }
                            
                            void Handle(const OrbeClientToServerGetRequest& message)
                            {
                                Client().IssueGet(message.Key());
                            }
                            
                            void Handle(const OrbeClientToServerPutRequest& message)
                            {
                                Client().IssuePut(message.Key(), message.Value());
                            }
                            
#ifdef OrbePartitionConnection_DEBUG_MESSAGES_INCLUDED
                            void Handle(const OrbeManualClientStatus& message)
                            {
                                m_partitionServer.DumpStats();
                            }
#endif
                            
                            void HandleGetCompleteItemFound(const TClientGetReplyValueType& getReply)
                            {
                                /*m_logger.Log("OrbeClientConnection::HandleGetCompleteItemFound: %d, %llu, %d\n",
                                 getReply.Value(),
                                 getReply.Timestamp(),
                                 getReply.SourceReplicaId());
                                 */
                                const auto msg(OrbeServerToClientGetResponse::CreateFound(getReply.Value()));
                                TBase::PostWrite(msg);
                            }
                            
                            void HandleGetCompleteItemNotFound()
                            {
                                //m_logger.Log("OrbeClientConnection::HandleGetCompleteItemNotFound\n");
                                const auto msg(OrbeServerToClientGetResponse::CreateNotFound());
                                TBase::PostWrite(msg);
                            }
                            
                            void HandlePutComplete(const TClientPutReplyValueType& putReply)
                            {
                                /*m_logger.Log("OrbeClientConnection::HandlePutComplete: %llu, %d\n",
                                 putReply.Timestamp(),
                                 putReply.SourceReplicaId());
                                 */
                                const auto msg(OrbeServerToClientPutResponse::Create());
                                TBase::PostWrite(msg);
                            }
                            
                        private:
                            void AttachClient(const TClientPtr& pClient)
                            {
                                SR_ASSERT(pClient != nullptr);
                                SR_ASSERT(m_pClient == nullptr);
                                m_pClient = pClient;
                            }
                            
                            OrbeClientConnection(boost::asio::io_service& ioService,
                                                 TPartitionServer& partitionServer,
                                                 TNetworkExchange& networkExchange,
                                                 TMetrics& metrics,
                                                 TLogger& logger)
                            : Utilities::Connection<ProtocolTraits, TSelf, Wire::OrbeMessageHeaderBytes>(ioService,
                                                                                                         logger,
                                                                                                         *this)
                            , m_pClient(nullptr)
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

