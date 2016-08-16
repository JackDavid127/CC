//
//  ServerNetworkTopologyModel.h
//  SimRunner
//
//  Created by Scott on 16/11/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/foreach.hpp>
#include "Protocols.h"
#include "Host.h"

namespace SimRunner
{
    namespace Utilities
    {
        namespace StandardConfig
        {
            class ServerNetworkTopologyModel
            {
                ServerNetworkTopologyModel(const std::string& partitionPort,
                                           const std::string& replicaPort,
                                           const std::string& clientPort,
                                           const std::vector<Host>& partitionNeighbours,
                                           const std::vector<Host>& replicatedPartitions,
                                           bool localTestRun)
                : m_partitionPort(partitionPort)
                , m_replicaPort(replicaPort)
                , m_clientPort(clientPort)
                , m_partitionNeighbours(partitionNeighbours)
                , m_replicatedPartitions(replicatedPartitions)
                , m_localTestRun(localTestRun)
                {
                    
                }
                
                std::string m_partitionPort;
                std::string m_replicaPort;
                std::string m_clientPort;
                std::vector<Host> m_partitionNeighbours;
                std::vector<Host> m_replicatedPartitions;
                bool m_localTestRun;
                
            public:
                const std::string& PartitionPort() const { return m_partitionPort; }
                
                const std::string& ReplicaPort() const { return m_replicaPort; }
                
                const std::string& ClientPort() const { return m_clientPort; }
                
                const std::vector<Host>& PartitionNeighbours() const { return m_partitionNeighbours; }
                
                const std::vector<Host>& ReplicatedPartitions() const { return m_replicatedPartitions; }
                
                bool LocalTestRun() const { return m_localTestRun; }
                
                static std::unique_ptr<ServerNetworkTopologyModel> FromFile(const std::string& file,
                                                                            const Protocols::PartitionIdType& partitionId,
                                                                            const Protocols::ReplicaIdType& replicaId)
                {
                    boost::property_tree::ptree pt;
                    boost::property_tree::read_json(file.c_str(), pt);
                    
                    const std::string partitionPort = pt.get<std::string>("partition_port");
                    const std::string replicaPort = pt.get<std::string>("replica_port");
                    const std::string clientPort = pt.get<std::string>("client_port");
                    
                    std::vector<Host> partitionNeighbours;
                    std::vector<Host> replicatedPartitions;
                    
                    Protocols::PartitionIdType p(0);
                    Protocols::ReplicaIdType r(0);
                    
                    BOOST_FOREACH(const boost::property_tree::ptree::value_type& replica, pt.get_child("replications"))
                    {
                        BOOST_FOREACH(const boost::property_tree::ptree::value_type& partition, replica.second.get_child(""))
                        {
                            std::string partitionHost = partition.second.get<std::string>("");
                            
                            if(r == replicaId)
                            {
                                partitionNeighbours.push_back(Host(partitionHost));
                            }
                            
                            if(p == partitionId)
                            {
                                replicatedPartitions.push_back(Host(partitionHost));
                            }
                            
                            ++ p;
                        }
                        
                        ++ r;
                        p = 0;
                    }
                    
                    const bool localTestRun = pt.get<bool>("local_test_run");
                    
                    return std::unique_ptr<ServerNetworkTopologyModel>(new ServerNetworkTopologyModel(partitionPort,
                                                                                                      replicaPort,
                                                                                                      clientPort,
                                                                                                      partitionNeighbours,
                                                                                                      replicatedPartitions,
                                                                                                      localTestRun));
                }
            };
        }
    }
}
