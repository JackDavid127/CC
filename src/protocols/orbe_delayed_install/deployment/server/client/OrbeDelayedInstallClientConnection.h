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
#include "OrbeDelayedInstallClientToServerGetRequest.h"
#include "OrbeDelayedInstallClientToServerPutRequest.h"
#include "OrbeDelayedInstallServerToClientGetResponse.h"
#include "OrbeDelayedInstallServerToClientPutResponse.h"
#include "Timing.h"

#define OrbeDelayedInstallPartitionConnection_DEBUG_MESSAGES_INCLUDED

#ifdef OrbeDelayedInstallPartitionConnection_DEBUG_MESSAGES_INCLUDED
#include "OrbeDelayedInstallManualClientHeartbeat.h"
#include "OrbeDelayedInstallManualClientStatus.h"
#endif

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
                    namespace Client
                    {
                        template<typename ProtocolTraits>
                        class OrbeDelayedInstallClientConnection : public Utilities::Connection<
                        ProtocolTraits,
                        OrbeDelayedInstallClientConnection<ProtocolTraits>,
                        Wire::OrbeDelayedInstallMessageHeaderBytes
                        >, private boost::noncopyable
                        {
                            typedef OrbeDelayedInstallClientConnection<ProtocolTraits> TSelf;
                            typedef typename Utilities::Connection<ProtocolTraits, TSelf, Wire::OrbeDelayedInstallMessageHeaderBytes> TBase;
                            typedef typename TBase::TLogger TLogger;
                            typedef typename ProtocolTraits::TMetrics TMetrics;
                            typedef typename ProtocolTraits::TPartitionServer TPartitionServer;
                            typedef typename ProtocolTraits::TNetworkExchange TNetworkExchange;
                            typedef typename ProtocolTraits::TClientGetReplyValueType TClientGetReplyValueType;
                            typedef typename ProtocolTraits::TClientPutReplyValueType TClientPutReplyValueType;
                            
                            typedef Wire::OrbeDelayedInstallClientToServerGetRequest<ProtocolTraits> OrbeDelayedInstallClientToServerGetRequest;
                            typedef Wire::OrbeDelayedInstallClientToServerPutRequest<ProtocolTraits> OrbeDelayedInstallClientToServerPutRequest;
                            typedef Wire::OrbeDelayedInstallServerToClientGetResponse<ProtocolTraits> OrbeDelayedInstallServerToClientGetResponse;
                            typedef Wire::OrbeDelayedInstallServerToClientPutResponse<ProtocolTraits> OrbeDelayedInstallServerToClientPutResponse;
                            
#ifdef OrbeDelayedInstallPartitionConnection_DEBUG_MESSAGES_INCLUDED
                            typedef Wire::OrbeDelayedInstallManualClientHeartbeat<ProtocolTraits> OrbeDelayedInstallManualClientHeartbeat;
                            typedef Wire::OrbeDelayedInstallManualClientStatus<ProtocolTraits> OrbeDelayedInstallManualClientStatus;
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
                                //m_logger.Log("OrbeDelayedInstallClientConnection::HandleConnectionEstablished\n");
                            }
                            
                            void ProcessBufferedMessage(const Utilities::SubBuffer& messageBytes)
                            {
                                //m_logger.Log("OrbeDelayedInstallClientConnection::ProcessBufferedMessage\n");
                                
                                const auto header = static_cast<Wire::OrbeDelayedInstallWireMessages>(messageBytes[0]);
                                
                                switch(header)
                                {
                                    case Wire::ClientToServerGetRequestMsg: {
                                        TBase::template ParseAndHandle<OrbeDelayedInstallClientToServerGetRequest>(messageBytes);
                                    } break;
                                    case Wire::ClientToServerPutRequestMsg: {
                                        TBase::template ParseAndHandle<OrbeDelayedInstallClientToServerPutRequest>(messageBytes);
                                    } break;
#ifdef OrbeDelayedInstallPartitionConnection_DEBUG_MESSAGES_INCLUDED
                                    case Wire::ManualClientHeartbeatMsg: {
                                        TBase::template ParseAndHandle<OrbeDelayedInstallManualClientHeartbeat>(messageBytes);
                                    } break;
                                    case Wire::ManualClientStatusMsg: {
                                        TBase::template ParseAndHandle<OrbeDelayedInstallManualClientStatus>(messageBytes);
                                    } break;
#endif
                                    default: {
                                        SR_ASSERT(false, "Client role does not handle this message type.\n");
                                    }
                                }
                            }
                            
                            void Handle(const OrbeDelayedInstallClientToServerGetRequest& message)
                            {
                                Client().IssueGet(message.Key());
                            }
                            
                            void Handle(const OrbeDelayedInstallClientToServerPutRequest& message)
                            {
                                Client().IssuePut(message.Key(), message.Value());
                            }
                            
                            void HandleGetCompleteItemFound(const TClientGetReplyValueType& getReply)
                            {
                                /*m_logger.Log("OrbeDelayedInstallClientConnection::HandleGetCompleteItemFound: %d, %llu, %d\n",
                                 getReply.Value(),
                                 getReply.Timestamp(),
                                 getReply.SourceReplicaId());
                                 */
                                const auto msg(OrbeDelayedInstallServerToClientGetResponse::CreateFound(getReply.Value()));
                                TBase::PostWrite(msg);
                            }
                            
                            void HandleGetCompleteItemNotFound()
                            {
                                //m_logger.Log("OrbeDelayedInstallClientConnection::HandleGetCompleteItemNotFound\n");
                                const auto msg(OrbeDelayedInstallServerToClientGetResponse::CreateNotFound());
                                TBase::PostWrite(msg);
                            }
                            
                            void HandlePutComplete(const TClientPutReplyValueType& putReply)
                            {
                                /*m_logger.Log("OrbeDelayedInstallClientConnection::HandlePutComplete: %llu, %d\n",
                                 putReply.Timestamp(),
                                 putReply.SourceReplicaId());
                                 */
                                const auto msg(OrbeDelayedInstallServerToClientPutResponse::Create());
                                TBase::PostWrite(msg);
                            }
                            
#ifdef OrbeDelayedInstallPartitionConnection_DEBUG_MESSAGES_INCLUDED
                            void Handle(const OrbeDelayedInstallManualClientHeartbeat& message)
                            {
                                m_partitionServer.SendHeartbeatVector();
                            }
                            
                            void Handle(const OrbeDelayedInstallManualClientStatus& message)
                            {
                                m_partitionServer.DumpStats();
                            }
#endif
                            
                        private:
                            void AttachClient(const TClientPtr& pClient)
                            {
                                SR_ASSERT(pClient != nullptr);
                                SR_ASSERT(m_pClient == nullptr);
                                m_pClient = pClient;
                            }
                            
                            OrbeDelayedInstallClientConnection(boost::asio::io_service& ioService,
                                                               TPartitionServer& partitionServer,
                                                               TNetworkExchange& networkExchange,
                                                               TMetrics& metrics,
                                                               TLogger& logger)
                            : Utilities::Connection<ProtocolTraits, TSelf, Wire::OrbeDelayedInstallMessageHeaderBytes>(ioService,
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

