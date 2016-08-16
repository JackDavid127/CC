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

namespace SimRunner
{
    namespace Protocols
    {
        namespace EC
        {
            namespace Deployment
            {
                namespace Client
                {
                    template<typename ProtocolTraits>
                    class ECClientToPartitionConnection : public Utilities::Connection<
                    ProtocolTraits,
                    ECClientToPartitionConnection<ProtocolTraits>,
                    Wire::ECMessageHeaderBytes
                    >, private boost::noncopyable
                    {
                        typedef ECClientToPartitionConnection<ProtocolTraits> TSelf;
                        typedef typename Utilities::Connection<ProtocolTraits, TSelf, Wire::ECMessageHeaderBytes> TBase;
                        typedef typename TBase::TLogger TLogger;
                        typedef typename ProtocolTraits::TKeyType TKeyType;
                        typedef typename ProtocolTraits::TRandomEngine TRandomEngine;
                        typedef typename ProtocolTraits::TClientRequestGenerator TClientRequestGenerator;
                        typedef typename ProtocolTraits::TConnectionStats TConnectionStats;
                        typedef typename ProtocolTraits::TMetricsStatsWriter TMetricsStatsWriter;
                        
                        typedef Wire::ECClientToServerGetRequest<ProtocolTraits> ECClientToServerGetRequest;
                        typedef Wire::ECClientToServerPutRequest<ProtocolTraits> ECClientToServerPutRequest;
                        typedef Wire::ECServerToClientGetResponse<ProtocolTraits> ECServerToClientGetResponse;
                        typedef Wire::ECServerToClientPutResponse<ProtocolTraits> ECServerToClientPutResponse;
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
                            m_logger.Log("ECClientToPartitionConnection::HandleConnectionEstablished\n");
                            SendNextRequest();
                        }
                        
                        void ProcessBufferedMessage(const Utilities::SubBuffer& messageBytes)
                        {
                            m_logger.Log("ECClientToPartitionConnection::ProcessBufferedMessage\n");
                            
                            const auto header = static_cast<Wire::ECWireMessages>(messageBytes[0]);
                            
                            m_connectionStats.RequestCompleted();
                            
                            switch(header)
                            {
                                case Wire::ServerToClientGetResponseMsg: {
                                    TBase::template ParseAndHandle<ECServerToClientGetResponse>(messageBytes);
                                } break;
                                case Wire::ServerToClientPutResponseMsg: {
                                    TBase::template ParseAndHandle<ECServerToClientPutResponse>(messageBytes);
                                } break;
                                default: {
                                    SR_ASSERT(false, "Client does not handle this message type.\n");
                                }
                            }
                        }
                        
                        void Handle(const ECServerToClientGetResponse& message)
                        {
                            SendNextRequest();
                        }
                        
                        void Handle(const ECServerToClientPutResponse& message)
                        {
                            SendNextRequest();
                        }
                        
                    private:
                        ECClientToPartitionConnection(boost::asio::io_service& ioService,
                                                      TRandomEngine& randomEngine,
                                                      TClientRequestGenerator& requestGenerator,
                                                      TLogger& logger,
                                                      TMetricsStatsWriter& statsWriter,
                                                      TClientIdType clientIdentifier)
                        : Utilities::Connection<ProtocolTraits, TSelf, Wire::ECMessageHeaderBytes>(ioService,
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
                            m_logger.Log("ECClientToPartitionConnection::SendNextRequest -- Get\n");
                            
                            TKeyType key(m_requestGenerator.NextKey());
                            
                            if(m_operationCategoryDistribution(m_randomEngine) >= 0.5)
                            {
                                const auto msg(ECClientToServerGetRequest::Create(key));
                                TBase::PostWrite(msg);
                                
                                m_connectionStats.RequestStarted(true);
                            }
                            else
                            {
                                const auto msg(ECClientToServerPutRequest::Create(key, "lmao"));
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

