//
//  BoltOnClientConnection.h
//  SimRunner
//
//  Created by Scott on 18/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#include <cstdlib>
#include <iostream>
#include <memory>
#include <vector>
#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include "BoltOnWire.h"
#include "Connection.h"
#include "BoltOnClientToServerGetRequest.h"
#include "BoltOnClientToServerGetsRequest.h"
#include "BoltOnClientToServerPutRequest.h"
#include "BoltOnServerToClientGetResponse.h"
#include "BoltOnServerToClientGetsResponse.h"
#include "BoltOnServerToClientPutResponse.h"
#include "Timing.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            namespace Deployment
            {
                namespace Server
                {
                    namespace Client
                    {
                        template<typename ProtocolTraits>
                        class BoltOnClientConnection : public Utilities::Connection<
                        ProtocolTraits,
                        BoltOnClientConnection<ProtocolTraits>,
                        Wire::BoltOnMessageHeaderBytes
                        >, private boost::noncopyable
                        {
                            typedef BoltOnClientConnection<ProtocolTraits> TSelf;
                            typedef typename Utilities::Connection<ProtocolTraits, TSelf, Wire::BoltOnMessageHeaderBytes> TBase;
                            typedef typename TBase::TLogger TLogger;
                            typedef typename ProtocolTraits::TMetrics TMetrics;
                            typedef typename ProtocolTraits::TValueWrapperPtr TValueWrapperPtr;
                            typedef typename ProtocolTraits::TShimDeployment TShimDeployment;
                            typedef typename ProtocolTraits::TBackingStorage TBackingStorage;
                            typedef typename ProtocolTraits::TKeyToPartitionMapper TKeyToPartitionMapper;
                            typedef typename ProtocolTraits::TPartitionServer TPartitionServer;
                            typedef typename ProtocolTraits::TNetworkExchange TNetworkExchange;
                            typedef typename ProtocolTraits::TValueWrapperDataFactory TValueWrapperDataFactory;
                            typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;

                            typedef Wire::BoltOnClientToServerGetRequest<ProtocolTraits> BoltOnClientToServerGetRequest;
                            typedef Wire::BoltOnClientToServerGetsRequest<ProtocolTraits> BoltOnClientToServerGetsRequest;
                            typedef Wire::BoltOnClientToServerPutRequest<ProtocolTraits> BoltOnClientToServerPutRequest;
                            typedef Wire::BoltOnServerToClientGetResponse<ProtocolTraits> BoltOnServerToClientGetResponse;
                            typedef Wire::BoltOnServerToClientGetsResponse<ProtocolTraits> BoltOnServerToClientGetsResponse;
                            typedef Wire::BoltOnServerToClientPutResponse<ProtocolTraits> BoltOnServerToClientPutResponse;

                            typedef typename ProtocolTraits::TBoltOnClient TClient;
                            typedef std::unique_ptr<TClient> TClientPtr;

                        public:
                            typedef typename TBase::TPtr TPtr;

                            static TPtr Create(ClientIdType clientId,
                                               TShimDeployment& shimDeployment,
                                               TKeyToPartitionMapper& keyToPartitionMapper,
                                               TPartitionServer& partitionServer,
                                               TNetworkExchange& networkExchange,
                                               boost::asio::io_service& ioService,
                                               TValueWrapperDataFactory& valueWrapperDataFactory,
                                               TMetrics& metrics,
                                               TLogger& logger)
                            {
                                return TPtr(new TSelf(clientId,
                                                      shimDeployment,
                                                      keyToPartitionMapper,
                                                      partitionServer,
                                                      networkExchange,
                                                      ioService,
                                                      valueWrapperDataFactory,
                                                      metrics,
                                                      logger));
                            }

                            void HandleConnectionEstablished()
                            {
                                m_logger.Log("BoltOnClientConnection::HandleConnectionEstablished\n");
                            }

                            void ProcessBufferedMessage(const Utilities::SubBuffer& messageBytes)
                            {
                                m_logger.Log("BoltOnClientConnection::ProcessBufferedMessage\n");

                                const auto header = static_cast<Wire::BoltOnWireMessages>(messageBytes[0]);

                                switch(header)
                                {
                                    case Wire::ClientToServerGetRequestMsg: {
                                        TBase::template ParseAndHandle<BoltOnClientToServerGetRequest>(messageBytes);
                                    } break;
                                    case Wire::ClientToServerGetsRequestMsg: {
                                        TBase::template ParseAndHandle<BoltOnClientToServerGetsRequest>(messageBytes);
                                    } break;
                                    case Wire::ClientToServerPutRequestMsg: {
                                        TBase::template ParseAndHandle<BoltOnClientToServerPutRequest>(messageBytes);
                                    } break;
                                    default: {
                                        SR_ASSERT(false, "Client role does not handle this message type.\n");
                                    }
                                }
                            }

                            void Handle(const BoltOnClientToServerGetRequest& message)
                            {
                                Client().IssueGet(message.Key());
                            }

                            void Handle(const BoltOnClientToServerPutRequest& message)
                            {
                                Client().IssuePut(message.Key(), message.Value());
                            }

                            void Handle(const BoltOnClientToServerGetsRequest& message)
                            {
                                Client().IssueGets(message.Keys());
                            }

                            void HandleGetCompleteItemFound(const TValueWrapperPtr getReply)
                            {
                                m_logger.Log("BoltOnClientConnection::HandleGetCompleteItemFound: %s\n",
                                             getReply->Value().c_str());

                                const auto msg(BoltOnServerToClientGetResponse::CreateFound(getReply->Value()));
                                TBase::PostWrite(msg);
                            }

                            void HandleGetCompleteItemNotFound()
                            {
                                m_logger.Log("BoltOnClientConnection::HandleGetCompleteItemNotFound\n");
                                const auto msg(BoltOnServerToClientGetResponse::CreateNotFound());
                                TBase::PostWrite(msg);
                            }
                            
                            void HandleGetsCompleteItemFound(const std::vector<TValueWrapperPtr> getReply)
                            {
                                m_logger.Log("BoltOnClientConnection::HandleGetsCompleteItemFound\n");
                                std::vector<TClientInputValueType> values;
                                for (TValueWrapperPtr p: getReply) values.push_back(p->Value());
                                const auto msg(BoltOnServerToClientGetsResponse::CreateFound(values));
                                TBase::PostWrite(msg);
                            }

                            void HandleGetsCompleteItemNotFound()
                            {
                                m_logger.Log("BoltOnClientConnection::HandleGetsCompleteItemNotFound\n");
                                const auto msg(BoltOnServerToClientGetsResponse::CreateNotFound());
                                TBase::PostWrite(msg);
                            }

                            void HandlePutComplete(const TValueWrapperPtr putReply)
                            {
                                m_logger.Log("BoltOnClientConnection::HandlePutComplete: %s\n",
                                             putReply->Value().c_str());

                                const auto msg(BoltOnServerToClientPutResponse::Create());
                                TBase::PostWrite(msg);
                            }

                        private:
                            BoltOnClientConnection(ClientIdType clientId,
                                                   TShimDeployment& shimDeployment,
                                                   TKeyToPartitionMapper& keyToPartitionMapper,
                                                   TPartitionServer& partitionServer,
                                                   TNetworkExchange& networkExchange,
                                                   boost::asio::io_service& ioService,
                                                   TValueWrapperDataFactory& valueWrapperDataFactory,
                                                   TMetrics& metrics,
                                                   TLogger& logger)
                            : Utilities::Connection<ProtocolTraits, TSelf, Wire::BoltOnMessageHeaderBytes>(ioService,
                                                                                                           logger,
                                                                                                           *this)
                            , m_backingStorage(clientId,
                                               keyToPartitionMapper,
                                               partitionServer,
                                               networkExchange,
                                               valueWrapperDataFactory)
                            , m_pClient(new TClient(clientId,
                                                    shimDeployment,
                                                    *this,
                                                    m_backingStorage,
                                                    metrics,
                                                    true))
                            , m_shimDeployment(shimDeployment)
                            , m_logger(logger)
                            {

                            }

                            TClient& Client() const { return *m_pClient; }

                            TBackingStorage m_backingStorage;
                            TClientPtr m_pClient;
                            TShimDeployment& m_shimDeployment;
                            TLogger& m_logger;
                        };
                    }
                }
            }
        }
    }
}
