//
//  BoltOnPartitionConnection.h
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
#include "BoltOnWire.h"
#include "Connection.h"
#include "BoltOnClientToServerGetRequest.h"
#include "BoltOnClientToServerGetsRequest.h"
#include "BoltOnClientToServerPutRequest.h"
#include "BoltOnServerToClientGetResponse.h"
#include "BoltOnServerToClientGetsResponse.h"
#include "BoltOnServerToClientPutResponse.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            namespace Deployment
            {
                namespace Client
                {
                    template<typename ProtocolTraits>
                    class BoltOnClientToPartitionConnection : public Utilities::Connection<
                    ProtocolTraits,
                    BoltOnClientToPartitionConnection<ProtocolTraits>,
                    Wire::BoltOnMessageHeaderBytes
                    >, private boost::noncopyable
                    {
                        typedef BoltOnClientToPartitionConnection<ProtocolTraits> TSelf;
                        typedef typename Utilities::Connection<ProtocolTraits, TSelf, Wire::BoltOnMessageHeaderBytes> TBase;
                        typedef typename TBase::TLogger TLogger;
                        typedef typename ProtocolTraits::TKeyType TKeyType;
                        typedef typename ProtocolTraits::TRandomEngine TRandomEngine;
                        typedef typename ProtocolTraits::TClientRequestGenerator TClientRequestGenerator;
                        typedef typename ProtocolTraits::TConnectionStats TConnectionStats;
                        typedef typename ProtocolTraits::TMetricsStatsWriter TMetricsStatsWriter;

                        typedef Wire::BoltOnClientToServerGetRequest<ProtocolTraits> ClientToServerGetRequest;
                        typedef Wire::BoltOnClientToServerGetsRequest<ProtocolTraits> ClientToServerGetsRequest;
                        typedef Wire::BoltOnClientToServerPutRequest<ProtocolTraits> ClientToServerPutRequest;
                        typedef Wire::BoltOnServerToClientGetResponse<ProtocolTraits> ServerToClientGetResponse;
                        typedef Wire::BoltOnServerToClientGetsResponse<ProtocolTraits> ServerToClientGetsResponse;
                        typedef Wire::BoltOnServerToClientPutResponse<ProtocolTraits> ServerToClientPutResponse;
                        typedef Protocols::ClientIdType TClientIdType;

                    public:
                        typedef typename TBase::TPtr TPtr;

                        static TPtr Create(boost::asio::io_service& ioService,
                                           TRandomEngine& randomEngine,
                                           TClientRequestGenerator& requestGenerator,
                                           TLogger& logger,
                                           TMetricsStatsWriter& statsWriter,
                                           TClientIdType clientIdentifier)
                        {
                            return TPtr(new TSelf(ioService,
                                                  randomEngine,
                                                  requestGenerator,
                                                  logger,
                                                  statsWriter,
                                                  clientIdentifier));
                        }

                        void HandleConnectionEstablished()
                        {
                            m_logger.Log("BoltOnClientToPartitionConnection::HandleConnectionEstablished\n");
                            SendNextRequest();
                        }

                        void ProcessBufferedMessage(const Utilities::SubBuffer& messageBytes)
                        {
                            m_logger.Log("BoltOnClientToPartitionConnection::ProcessBufferedMessage\n");

                            const auto header = static_cast<Wire::BoltOnWireMessages>(messageBytes[0]);

                            m_connectionStats.RequestCompleted();

                            switch(header)
                            {
                                case Wire::ServerToClientGetResponseMsg: {
                                    TBase::template ParseAndHandle<ServerToClientGetResponse>(messageBytes);
                                } break;
                                case Wire::ServerToClientGetsResponseMsg: {
                                    TBase::template ParseAndHandle<ServerToClientGetsResponse>(messageBytes);
                                } break;
                                case Wire::ServerToClientPutResponseMsg: {
                                    TBase::template ParseAndHandle<ServerToClientPutResponse>(messageBytes);
                                } break;
                                default: {
                                    SR_ASSERT(false, "Client does not handle this message type.\n");
                                }
                            }
                        }

                        void Handle(const ServerToClientGetResponse& message)
                        {
                            SendNextRequest();
                        }

                        void Handle(const ServerToClientGetsResponse& message)
                        {
                            SendNextRequest();
                        }

                        void Handle(const ServerToClientPutResponse& message)
                        {
                            SendNextRequest();
                        }

                    private:
                        BoltOnClientToPartitionConnection(boost::asio::io_service& ioService,
                                                          TRandomEngine& randomEngine,
                                                          TClientRequestGenerator& requestGenerator,
                                                          TLogger& logger,
                                                          TMetricsStatsWriter& statsWriter,
                                                          TClientIdType clientIdentifier)
                        : Utilities::Connection<ProtocolTraits, TSelf, Wire::BoltOnMessageHeaderBytes>(ioService,
                                                                                                       logger,
                                                                                                       *this)
                        , m_randomEngine(randomEngine)
                        , m_requestGenerator(requestGenerator)
                        , m_logger(logger)
                        , m_operationCategoryDistribution(0.0, 1.f)
                        , m_connectionStats(statsWriter,
                                            clientIdentifier,
                                            DurationBetweenLogsSeconds)
                        {

                        }

                        void SendNextRequest()
                        {
                            m_logger.Log("BoltOnClientToPartitionConnection::SendNextRequest -- Get\n");

                            TKeyType key(m_requestGenerator.NextKey());

                            if(m_operationCategoryDistribution(m_randomEngine) >= 0.5)
                            {
                                const auto msg(ClientToServerGetRequest::Create(key));
                                TBase::PostWrite(msg);

                                m_connectionStats.RequestStarted(true);
                            }
                            else
                            {
                                const auto msg(ClientToServerPutRequest::Create(key, "lmao"));
                                TBase::PostWrite(msg);

                                m_connectionStats.RequestStarted(false);
                            }
                        }

                        TRandomEngine& m_randomEngine;
                        TClientRequestGenerator& m_requestGenerator;
                        TLogger& m_logger;
                        std::uniform_real_distribution<double> m_operationCategoryDistribution;

                        TConnectionStats m_connectionStats;
                        static constexpr double DurationBetweenLogsSeconds = 10.0;
                    };
                }
            }
        }
    }
}
