//
//  BoltOnClientConnectionFactory.h
//  SimRunner
//
//  Created by Scott on 18/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <cstdlib>
#include <iostream>
#include <memory>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>

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
                        template <typename ProtocolTraits>
                        class BoltOnClientConnectionFactory
                        {
                            typedef typename ProtocolTraits::TClientConnection TClientConnection;
                            typedef typename ProtocolTraits::TMetrics TMetrics;
                            typedef typename ProtocolTraits::TLogger TLogger;
                            typedef typename ProtocolTraits::TShimDeployment TShimDeployment;
                            typedef typename ProtocolTraits::TPartitionServer TPartitionServer;
                            typedef typename ProtocolTraits::TNetworkExchange TNetworkExchange;
                            typedef typename ProtocolTraits::TKeyToPartitionMapper TKeyToPartitionMapper;
                            typedef typename ProtocolTraits::TValueWrapperDataFactory TValueWrapperDataFactory;
                            
                        public:
                            typedef typename TClientConnection::TPtr TConnectionPtr;
                            
                            BoltOnClientConnectionFactory(TShimDeployment& shimDeployment,
                                                          TPartitionServer& partitionServer,
                                                          TNetworkExchange& networkExchange,
                                                          TKeyToPartitionMapper& keyToPartitionMapper,
                                                          boost::asio::io_service& ioService,
                                                          TValueWrapperDataFactory& valueWrapperDataFactory,
                                                          TMetrics& metrics,
                                                          TLogger& logger)
                            : m_nextClientId(0)
                            , m_shimDeployment(shimDeployment)
                            , m_partitionServer(partitionServer)
                            , m_networkExchange(networkExchange)
                            , m_keyToPartitionMapper(keyToPartitionMapper)
                            , m_ioService(ioService)
                            , m_valueWrapperDataFactory(valueWrapperDataFactory)
                            , m_metrics(metrics)
                            , m_logger(logger)
                            {
                                
                            }
                            
                            TConnectionPtr Create()
                            {
                                return TClientConnection::Create(m_nextClientId ++,
                                                                 m_shimDeployment,
                                                                 m_keyToPartitionMapper,
                                                                 m_partitionServer,
                                                                 m_networkExchange,
                                                                 m_ioService,
                                                                 m_valueWrapperDataFactory,
                                                                 m_metrics,
                                                                 m_logger);
                            }
                            
                        private:
                            Protocols::ClientIdType m_nextClientId;
                            TShimDeployment& m_shimDeployment;
                            TPartitionServer& m_partitionServer;
                            TNetworkExchange& m_networkExchange;
                            TKeyToPartitionMapper& m_keyToPartitionMapper;
                            boost::asio::io_service& m_ioService;
                            TValueWrapperDataFactory& m_valueWrapperDataFactory;
                            TMetrics& m_metrics;
                            TLogger& m_logger;
                        };
                        
                        template <typename ProtocolTraits>
                        class BoltOnClientConnectionFactoryFactory
                        {
                            typedef typename ProtocolTraits::TMetrics TMetrics;
                            typedef typename ProtocolTraits::TLogger TLogger;
                            typedef typename ProtocolTraits::TPartitionServer TPartitionServer;
                            typedef typename ProtocolTraits::TNetworkExchange TNetworkExchange;
                            typedef typename ProtocolTraits::TShimDeployment TShimDeployment;
                            typedef typename ProtocolTraits::TKeyToPartitionMapper TKeyToPartitionMapper;
                            typedef typename ProtocolTraits::TClientConnectionFactory TClientConnectionFactory;
                            typedef typename ProtocolTraits::TValueWrapperDataFactory TValueWrapperDataFactory;
                            
                        public:
                            BoltOnClientConnectionFactoryFactory(TKeyToPartitionMapper& keyToPartitionMapper,
                                                                 TValueWrapperDataFactory& valueWrapperDataFactory,
                                                                 TMetrics& metrics,
                                                                 TLogger& logger)
                            : m_keyToPartitionMapper(keyToPartitionMapper)
                            , m_valueWrapperDataFactory(valueWrapperDataFactory)
                            , m_metrics(metrics)
                            , m_logger(logger)
                            {
                                
                            }
                            
                            void SetShim(TShimDeployment& shimDeployment)
                            {
                                m_pShimDeployment = &shimDeployment;
                            }
                            
                            std::unique_ptr<TClientConnectionFactory> Create(TPartitionServer& partitionServer,
                                                                             TNetworkExchange& networkExchange,
                                                                             boost::asio::io_service& ioService)
                            {
                                return std::unique_ptr<TClientConnectionFactory>(new TClientConnectionFactory(*m_pShimDeployment,
                                                                                                              partitionServer,
                                                                                                              networkExchange,
                                                                                                              m_keyToPartitionMapper,
                                                                                                              ioService,
                                                                                                              m_valueWrapperDataFactory,
                                                                                                              m_metrics,
                                                                                                              m_logger));
                            }
                            
                        private:
                            TShimDeployment* m_pShimDeployment;
                            TKeyToPartitionMapper& m_keyToPartitionMapper;
                            TValueWrapperDataFactory& m_valueWrapperDataFactory;
                            TMetrics& m_metrics;
                            TLogger& m_logger;
                        };
                    }
                }
            }
        }
    }
}
