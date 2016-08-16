//
//  ECReplicaConnectionFactory.h
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
#include "ECPartitionConnection.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace EC
        {
            namespace Deployment
            {
                namespace Server
                {
                    namespace Replica
                    {
                        template <typename ProtocolTraits>
                        class ECReplicaConnectionFactory
                        {
                            typedef typename ProtocolTraits::TReplicaConnection TReplicaConnection;
                            typedef typename ProtocolTraits::TLogger TLogger;
                            typedef typename ProtocolTraits::TPartitionServer TPartitionServer;
                            typedef typename ProtocolTraits::TNetworkExchange TNetworkExchange;
                            typedef typename ProtocolTraits::TBroadcaster TBroadcaster;
                            
                        public:
                            typedef typename TReplicaConnection::TPtr TConnectionPtr;
                            
                            ECReplicaConnectionFactory(TPartitionServer& partitionServer,
                                                       TBroadcaster& broadcaster,
                                                       TNetworkExchange& networkExchange,
                                                       boost::asio::io_service& ioService,
                                                       TLogger& logger)
                            : m_partitionServer(partitionServer)
                            , m_broadcaster(broadcaster)
                            , m_networkExchange(networkExchange)
                            , m_ioService(ioService)
                            , m_logger(logger)
                            {
                                
                            }
                            
                            TConnectionPtr Create()
                            {   
                                return TReplicaConnection::Create(m_ioService,
                                                                  m_partitionServer,
                                                                  m_broadcaster,
                                                                  m_networkExchange,
                                                                  m_logger);
                            }
                            
                        private:
                            TPartitionServer& m_partitionServer;
                            TBroadcaster& m_broadcaster;
                            TNetworkExchange& m_networkExchange;
                            boost::asio::io_service& m_ioService;
                            TLogger& m_logger;
                        };
                    }
                }
            }
        }
    }
}
