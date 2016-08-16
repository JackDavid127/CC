//
//  ConnectionListener.h
//  SimRunner
//
//  Created by Scott on 21/10/2014.
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
    namespace Utilities
    {
        template <typename TConnectionFactory>
        class ConnectionListener : boost::noncopyable
        {
            typedef typename TConnectionFactory::TConnectionPtr TConnectionPtr;
            
        public:
            ConnectionListener(boost::asio::io_service& ioService,
                               uint16_t port,
                               TConnectionFactory& factory)
            : m_tcpAcceptor(ioService, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port))
            , m_factory(factory)
            {
                StartAccept();
            }
            
        private:
            void StartAccept()
            {
                TConnectionPtr pConnection(m_factory.Create());
                
                m_tcpAcceptor.async_accept(pConnection->Socket(),
                                           boost::bind(&ConnectionListener::HandleAccept,
                                                       this,
                                                       pConnection,
                                                       boost::asio::placeholders::error));
            }
            
            void HandleAccept(TConnectionPtr pConnection,
                              const boost::system::error_code& error)
            {
                if (!error)
                {
                    pConnection->Start();
                    
                    // disable nagle for this connection
                    pConnection->Socket().set_option( boost::asio::ip::tcp::no_delay( true) );
                    //pConnection->Socket().set_option( boost::asio::socket_base::send_buffer_size( 65536 ) );
                    //pConnection->Socket().set_option( boost::asio::socket_base::receive_buffer_size( 65536 ) );
                }
                
                StartAccept();
            }
            
            boost::asio::ip::tcp::acceptor m_tcpAcceptor;
            TConnectionFactory& m_factory;
        };
    }
}

