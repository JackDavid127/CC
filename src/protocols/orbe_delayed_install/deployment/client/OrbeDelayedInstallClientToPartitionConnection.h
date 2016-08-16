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

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            namespace Deployment
            {
                namespace Client
                {
                    template<typename ProtocolTraits>
                    class OrbeDelayedInstallClientToPartitionConnection : public Utilities::Connection<
                    ProtocolTraits,
                    OrbeDelayedInstallClientToPartitionConnection<ProtocolTraits>,
                    Wire::OrbeDelayedInstallMessageHeaderBytes
                    >, private boost::noncopyable
                    {
                        typedef OrbeDelayedInstallClientToPartitionConnection<ProtocolTraits> TSelf;
                        
                        typedef typename Utilities::Connection<ProtocolTraits, TSelf, Wire::OrbeDelayedInstallMessageHeaderBytes> TBase;
                        typedef typename TBase::TLogger TLogger;
                        typedef typename ProtocolTraits::TKeyType TKeyType;
                        typedef typename ProtocolTraits::TRandomEngine TRandomEngine;
                        typedef typename ProtocolTraits::TClientRequestGenerator TClientRequestGenerator;
                        typedef typename ProtocolTraits::TConnectionStats TConnectionStats;
                        typedef typename ProtocolTraits::TMetricsStatsWriter TMetricsStatsWriter;
                        
                        typedef Wire::OrbeDelayedInstallClientToServerGetRequest<ProtocolTraits> OrbeDelayedInstallClientToServerGetRequest;
                        typedef Wire::OrbeDelayedInstallClientToServerPutRequest<ProtocolTraits> OrbeDelayedInstallClientToServerPutRequest;
                        typedef Wire::OrbeDelayedInstallServerToClientGetResponse<ProtocolTraits> OrbeDelayedInstallServerToClientGetResponse;
                        typedef Wire::OrbeDelayedInstallServerToClientPutResponse<ProtocolTraits> OrbeDelayedInstallServerToClientPutResponse;
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
                            //m_logger.Log("OrbeDelayedInstallClientToPartitionConnection::HandleConnectionEstablished\n");
                            SendNextRequest();
                        }
                        
                        void ProcessBufferedMessage(const Utilities::SubBuffer& messageBytes)
                        {
                            //m_logger.Log("OrbeDelayedInstallClientToPartitionConnection::ProcessBufferedMessage\n");
                            
                            const auto header = static_cast<Wire::OrbeDelayedInstallWireMessages>(messageBytes[0]);
                            
                            m_connectionStats.RequestCompleted();
                            
                            switch(header)
                            {
                                case Wire::ServerToClientGetResponseMsg: {
                                    TBase::template ParseAndHandle<OrbeDelayedInstallServerToClientGetResponse>(messageBytes);
                                } break;
                                case Wire::ServerToClientPutResponseMsg: {
                                    TBase::template ParseAndHandle<OrbeDelayedInstallServerToClientPutResponse>(messageBytes);
                                } break;
                                default: {
                                    SR_ASSERT(false, "Client does not handle this message type.\n");
                                }
                            }
                        }
                        
                        void Handle(const OrbeDelayedInstallServerToClientGetResponse& message)
                        {
                            SendNextRequest();
                        }
                        
                        void Handle(const OrbeDelayedInstallServerToClientPutResponse& message)
                        {
                            SendNextRequest();
                        }
                        
                    private:
                        OrbeDelayedInstallClientToPartitionConnection(boost::asio::io_service& ioService,
                                                                      TRandomEngine& randomEngine,
                                                                      TClientRequestGenerator& requestGenerator,
                                                                      TLogger& logger,
                                                                      TMetricsStatsWriter& statsWriter,
                                                                      TClientIdType clientIdentifier)
                        : Utilities::Connection<ProtocolTraits, TSelf, Wire::OrbeDelayedInstallMessageHeaderBytes>(ioService,
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
                            m_logger.Log("OrbeDelayedInstallClientToPartitionConnection::SendNextRequest\n");
                            
                            TKeyType key(m_requestGenerator.NextKey());
                            
                            if(m_operationCategoryDistribution(m_randomEngine) >= 0.5)
                            {
                                const auto msg(OrbeDelayedInstallClientToServerGetRequest::Create(key));
                                TBase::PostWrite(msg);
                                
                                m_connectionStats.RequestStarted(true);
                            }
                            else
                            {
                                const auto msg(OrbeDelayedInstallClientToServerPutRequest::Create(key, 1234));
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

