//
//  OrbeClientConnectionFactory.h
//  SimRunner
//
//  Created by Scott on 20/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <string>
#include <cstdlib>
#include <iostream>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>

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
                    template <typename ProtocolTraits>
                    class OrbeClientToPartitionConnectionFactory
                    {
                        typedef typename ProtocolTraits::TConnection TConnection;
                        typedef typename ProtocolTraits::TClientRequestGenerator TClientRequestGenerator;
                        typedef typename ProtocolTraits::TRandomEngine TRandomEngine;
                        typedef typename ProtocolTraits::TLogger TLogger;
                        typedef typename ProtocolTraits::TMetricsStatsWriter TMetricsStatsWriter;
                        
                        typedef Protocols::ClientIdType TClientIdType;
                        
                    public:
                        typedef typename TConnection::TPtr TConnectionPtr;
                        
                        OrbeClientToPartitionConnectionFactory(boost::asio::io_service& ioService,
                                                               TRandomEngine& randomEngine,
                                                               TClientRequestGenerator& requestGenerator,
                                                               TLogger& logger,
                                                               const std::string& targetHost)
                        : m_ioService(ioService)
                        , m_randomEngine(randomEngine)
                        , m_requestGenerator(requestGenerator)
                        , m_logger(logger)
                        , m_connectionCounter(0)
                        , m_statsWriter("./logs/connection/" + targetHost + "/")
                        {
                            
                        }
                        
                        TConnectionPtr Create()
                        {
                            return TConnection::Create(m_ioService,
                                                       m_randomEngine,
                                                       m_requestGenerator,
                                                       m_logger,
                                                       m_statsWriter,
                                                       m_connectionCounter ++);
                        }
                        
                    private:
                        boost::asio::io_service& m_ioService;
                        TRandomEngine& m_randomEngine;
                        TClientRequestGenerator& m_requestGenerator;
                        TLogger& m_logger;
                        TMetricsStatsWriter m_statsWriter;
                        TClientIdType m_connectionCounter;
                    };
                }
            }
        }
    }
}
