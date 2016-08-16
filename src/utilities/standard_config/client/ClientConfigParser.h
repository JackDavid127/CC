//
//  ClientConfigParser.h
//  SimRunner
//
//  Created by Scott on 25/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <string>
#include <boost/program_options.hpp>
#include "ClientConfig.h"

namespace SimRunner
{
    namespace Utilities
    {
        namespace StandardConfig
        {
            namespace Client
            {
                inline Config ParseConfig(int argc, const char * argv[], const char * arge[])
                {
                    typedef uint16_t TPort;
                    typedef std::string THost;
                    typedef uint16_t TConnections;
                    
                    // Declare the supported options.
                    namespace po = boost::program_options;
                    po::options_description desc("Allowed options");
                    desc.add_options()
                    ("help", "produce help message")
                    ("host", po::value<THost>(), "Server host")
                    ("port", po::value<TPort>(), "Server listen port")
                    ("connections", po::value<TConnections>(), "Server connections count")
                    ;
                    
                    po::variables_map vm;
                    po::parsed_options parsed = po::command_line_parser(argc, argv).options(desc).allow_unregistered().run();
                    po::store(parsed, vm);
                    po::notify(vm);
                    
                    if (vm.count("help"))
                    {
                        std::cout << desc << "\n";
                        return Config::Invalid();
                    }
                    
                    THost host;
                    if (vm.count("host"))
                    {
                        host = vm["host"].as<THost>();
                        std::cout << "Host set to " <<  host << ".\n";
                    }
                    else
                    {
                        std::cout << "Error: no host set via --host option.\n";
                        return Config::Invalid();
                    }
                    
                    TPort port;
                    if (vm.count("port"))
                    {
                        port = vm["port"].as<TPort>();
                        std::cout << "Port set to " <<  port << ".\n";
                    }
                    else
                    {
                        std::cout << "Error: no port set via --port option.\n";
                        return Config::Invalid();
                    }
                    
                    TConnections connections;
                    if (vm.count("connections"))
                    {
                        connections = vm["connections"].as<TConnections>();
                        std::cout << "Num connections set to " <<  connections << ".\n";
                    }
                    else
                    {
                        std::cout << "Error: no connections count set via --connections option.\n";
                        return Config::Invalid();
                    }
                    
                    return Config(host, port, connections);
                };
            }
        }
    }
}
