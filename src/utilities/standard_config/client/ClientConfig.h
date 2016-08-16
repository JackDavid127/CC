//
//  ClientConfig.h
//  SimRunner
//
//  Created by Scott on 25/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <string>

namespace SimRunner
{
    namespace Utilities
    {
        namespace StandardConfig
        {
            namespace Client
            {
                class Config
                {
                public:
                    static Config Invalid()
                    {
                        return Config();
                    }
                    
                    Config(const std::string& host, uint16_t port, uint16_t connections)
                    : m_valid(true)
                    , m_host(host)
                    , m_port(port)
                    , m_connections(connections)
                    {
                        
                    }
                    
                    bool Valid() const { return m_valid; }
                    
                    const std::string& Host() const { return m_host; }
                    
                    uint16_t Port() const { return m_port; }
                    
                    uint16_t Connections() const { return m_connections; }
                    
                private:
                    Config()
                    : m_valid(false)
                    , m_host()
                    , m_port()
                    , m_connections()
                    {
                        
                    }
                    
                    bool m_valid;
                    std::string m_host;
                    uint16_t m_port;
                    uint16_t m_connections;
                };
            }
        }
    }
}
