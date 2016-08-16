//
//  OrbeDelayedInstallClientConnectionFactory.h
//  SimRunner
//
//  Created by Scott on 20/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

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
        namespace OrbeDelayedInstall
        {
            namespace Deployment
            {
                namespace Server
                {
                    namespace Client
                    {
                        template <typename ProtocolTraits>
                        class OrbeDelayedInstallClientConnectionFactory
                        {
                            typedef typename ProtocolTraits::TClientConnection TClientConnection;
                            typedef typename ProtocolTraits::TMetrics TMetrics;
                            typedef typename ProtocolTraits::TLogger TLogger;
                            typedef typename ProtocolTraits::TPartitionServer TPartitionServer;
                            typedef typename ProtocolTraits::TNetworkExchange TNetworkExchange;
                            typedef typename ProtocolTraits::TKeyToPartitionMapper TKeyToPartitionMapper;
                            
                        public:
                            typedef typename TClientConnection::TPtr TConnectionPtr;
                            
                            OrbeDelayedInstallClientConnectionFactory(TKeyToPartitionMapper& keyToPartitionMapper,
                                                      TPartitionServer& partitionServer,
                                                      TNetworkExchange& networkExchange,
                                                      boost::asio::io_service& ioService,
                                                      TMetrics& metrics,
                                                      TLogger& logger)
                            : m_nextClientId(0)
                            , m_keyToPartitionMapper(keyToPartitionMapper)
                            , m_partitionServer(partitionServer)
                            , m_networkExchange(networkExchange)
                            , m_ioService(ioService)
                            , m_metrics(metrics)
                            , m_logger(logger)
                            {
                                
                            }
                            
                            TConnectionPtr Create()
                            {   
                                return TClientConnection::Create(m_nextClientId ++,
                                                                 m_keyToPartitionMapper,
                                                                 m_ioService,
                                                                 m_partitionServer,
                                                                 m_networkExchange,
                                                                 m_metrics,
                                                                 m_logger);
                            }
                            
                        private:
                            Protocols::ClientIdType m_nextClientId;
                            TKeyToPartitionMapper& m_keyToPartitionMapper;
                            TPartitionServer& m_partitionServer;
                            TNetworkExchange& m_networkExchange;
                            boost::asio::io_service& m_ioService;
                            TMetrics& m_metrics;
                            TLogger& m_logger;
                        };
                    }
                }
            }
        }
    }
}
