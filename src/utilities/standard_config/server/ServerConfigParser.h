//
//  ServerConfigParser.h
//  SimRunner
//
//  Created by Scott on 25/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <string>
#include <boost/program_options.hpp>
#include "ServerNetworkTopologyModel.h"
#include "ServerConfig.h"

namespace SimRunner
{
    namespace Utilities
    {
        namespace StandardConfig
        {
            namespace Server
            {
                inline Config ParseConfig(int argc, const char * argv[], const char * arge[])
                {
                    // Declare the supported options.
                    namespace po = boost::program_options;
                    po::options_description desc("Allowed options");
                    desc.add_options()
                    ("help", "produce help message")
                    ("p", po::value<SimRunner::Protocols::PartitionIdType>(), "Partition ID")
                    ("r", po::value<SimRunner::Protocols::ReplicaIdType>(), "Replica ID")
                    ("config", po::value<std::string>(), "Config Path")
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
                    
                    SimRunner::Protocols::PartitionIdType inputPartitionId;
                    if (vm.count("p"))
                    {
                        inputPartitionId = vm["p"].as<SimRunner::Protocols::PartitionIdType>();
                    }
                    else
                    {
                        inputPartitionId = 0;
                        std::cout << "Partition ID was not set - defaulting to 0.\n";
                    }
                    
                    SimRunner::Protocols::ReplicaIdType inputReplicaId;
                    if (vm.count("r"))
                    {
                        inputReplicaId = vm["r"].as<SimRunner::Protocols::ReplicaIdType>();
                    }
                    else
                    {
                        inputReplicaId = 0;
                        std::cout << "Replica ID was not set - defaulting to 0.\n";
                    }
                    
                    std::string configPathStr;
                    if (vm.count("config"))
                    {
                        configPathStr = vm["config"].as<std::string>();
                    }
                    else
                    {
                        configPathStr = "./home_server_config.json";
                        std::cout << "Config was not set - defaulting to ./home_server_config.json.\n";
                    }
                    
                    return Config(inputPartitionId,
                                  inputReplicaId,
                                  SimRunner::Utilities::StandardConfig::ServerNetworkTopologyModel::FromFile(configPathStr,
                                                                                                             inputPartitionId,
                                                                                                             inputReplicaId));
                };
            }
        }
    }
}
