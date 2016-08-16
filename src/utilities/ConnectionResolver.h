//
//  ConnectionResolver.h
//  SimRunner
//
//  Created by Scott on 20/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <string>
#include <cstdlib>
#include <iostream>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/lexical_cast.hpp>

namespace SimRunner
{
    namespace Utilities
    {
        template <typename ProtocolTraits, typename TConnectionFactory>
        class ConnectionResolver
        {
            typedef typename ProtocolTraits::TLogger TLogger;
            typedef typename TConnectionFactory::TConnectionPtr TConnectionPtr;
            
        public:
            ConnectionResolver(TLogger& logger)
            : m_logger(logger)
            {
                
            }
            
            bool TryConnectSynchronous(TConnectionFactory& connectionFactory,
                                       boost::asio::io_service& ioService,
                                       const std::string& hostName,
                                       uint16_t port)
            {
                TConnectionPtr pNewSession(connectionFactory.Create());
                
                std::string portStr(boost::lexical_cast<std::string>(port));
                boost::asio::ip::tcp::resolver resolver(ioService);
                boost::asio::ip::tcp::resolver::query query(hostName, portStr);
                boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query);
                
                boost::system::error_code error;
                boost::asio::ip::tcp::resolver::iterator connectionIter = boost::asio::connect(pNewSession->Socket(),
                                                                                               iter,
                                                                                               error);
                return HandleConnect(pNewSession, error);
            }
            
            void TryConnectAsynchronous(TConnectionFactory& connectionFactory,
                                        boost::asio::io_service& ioService,
                                        const std::string& hostName,
                                        uint16_t port)
            {
                TConnectionPtr pNewSession(connectionFactory.Create());
                
                std::string portStr(boost::lexical_cast<std::string>(port));
                boost::asio::ip::tcp::resolver resolver(ioService);;
                boost::asio::ip::tcp::resolver::query query(hostName, portStr);
                boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query);
                
                boost::asio::async_connect(pNewSession->Socket(),
                                           iter,
                                           boost::bind(&HandleConnect,
                                                       pNewSession,
                                                       boost::asio::placeholders::error));
            }
            
        private:
            bool HandleConnect(TConnectionPtr pNewSession,
                               const boost::system::error_code& error)
            {
                if (error)
                {
                    m_logger.Log("Connection Error.\n");
                    return false;
                }
                else
                {
                    m_logger.Log("Connection Made.\n");
                    pNewSession->Start();
                    return true;
                }
            }
            
            TLogger& m_logger;
        };
    }
}
