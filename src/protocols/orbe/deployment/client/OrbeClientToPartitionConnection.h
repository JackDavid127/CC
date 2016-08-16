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

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            namespace Deployment
            {
                namespace Client
                {
                    template<typename ProtocolTraits>
                    class OrbeClientToPartitionConnection : public Utilities::Connection<
                    ProtocolTraits,
                    OrbeClientToPartitionConnection<ProtocolTraits>,
                    Wire::OrbeMessageHeaderBytes
                    >, private boost::noncopyable
                    {
                        typedef OrbeClientToPartitionConnection<ProtocolTraits> TSelf;

                        typedef typename Utilities::Connection<ProtocolTraits, TSelf, Wire::OrbeMessageHeaderBytes> TBase;
                        typedef typename TBase::TLogger TLogger;
                        typedef typename ProtocolTraits::TKeyType TKeyType;
                        typedef typename ProtocolTraits::TRandomEngine TRandomEngine;
                        typedef typename ProtocolTraits::TClientRequestGenerator TClientRequestGenerator;
                        typedef typename ProtocolTraits::TConnectionStats TConnectionStats;
                        typedef typename ProtocolTraits::TMetricsStatsWriter TMetricsStatsWriter;
                        
                        typedef Wire::OrbeClientToServerGetRequest<ProtocolTraits> OrbeClientToServerGetRequest;
                        typedef Wire::OrbeClientToServerPutRequest<ProtocolTraits> OrbeClientToServerPutRequest;
                        typedef Wire::OrbeServerToClientGetResponse<ProtocolTraits> OrbeServerToClientGetResponse;
                        typedef Wire::OrbeServerToClientPutResponse<ProtocolTraits> OrbeServerToClientPutResponse;
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
                            auto pResult = TPtr(new TSelf(ioService, randomEngine, requestGenerator, logger, statsWriter, clientIdentifier));
                            pResult->SetupSharedPtrToSelf();
                            return pResult;
                        }
                        
                        void HandleConnectionEstablished()
                        {
                            //m_logger.Log("OrbeClientToPartitionConnection::HandleConnectionEstablished\n");
                            SendNextRequest();
                        }
                        
                        void ProcessBufferedMessage(const Utilities::SubBuffer& messageBytes)
                        {
                            //m_logger.Log("OrbeClientToPartitionConnection::ProcessBufferedMessage\n");
                            
                            const auto header = static_cast<Wire::OrbeWireMessages>(messageBytes[0]);
                            
                            m_connectionStats.RequestCompleted();
                            
                            switch(header)
                            {
                                case Wire::ServerToClientGetResponseMsg: {
                                    TBase::template ParseAndHandle<OrbeServerToClientGetResponse>(messageBytes);
                                } break;
                                case Wire::ServerToClientPutResponseMsg: {
                                    TBase::template ParseAndHandle<OrbeServerToClientPutResponse>(messageBytes);
                                } break;
                                default: {
                                    SR_ASSERT(false, "Client does not handle this message type.\n");
                                }
                            }
                        }
                        
                        void Handle(const OrbeServerToClientGetResponse& message)
                        {
                            SendNextRequest();
                        }
                        
                        void Handle(const OrbeServerToClientPutResponse& message)
                        {
                            SendNextRequest();
                        }
                        
                    private:
                        OrbeClientToPartitionConnection(boost::asio::io_service& ioService,
                                                        TRandomEngine& randomEngine,
                                                        TClientRequestGenerator& requestGenerator,
                                                        TLogger& logger,
                                                        TMetricsStatsWriter& statsWriter,
                                                        TClientIdType clientIdentifier)
                        : Utilities::Connection<ProtocolTraits, TSelf, Wire::OrbeMessageHeaderBytes>(ioService,
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
                            m_logger.Log("OrbeClientToPartitionConnection::SendNextRequest -- Get\n");
                            
                            TKeyType key(m_requestGenerator.NextKey());
                            
                            if(m_operationCategoryDistribution(m_randomEngine) >= 0.5)
                            {
                                const auto msg(OrbeClientToServerGetRequest::Create(key));
                                TBase::PostWrite(msg);
                                
                                m_connectionStats.RequestStarted(true);
                            }
                            else
                            {
                                const auto msg(OrbeClientToServerPutRequest::Create(key, 1234));
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

