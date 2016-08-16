//
//  ServerConfig.h
//  SimRunner
//
//  Created by Scott on 25/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <memory>
#include <utility>
#include "ServerNetworkTopologyModel.h"
#include "Protocols.h"

namespace SimRunner
{
    namespace Utilities
    {
        namespace StandardConfig
        {
            namespace Server
            {
                class Config
                {
                public:
                    static Config Invalid()
                    {
                        return Config();
                    }
                    
                    Config(Protocols::PartitionIdType partitionId,
                           Protocols::ReplicaIdType replicaId,
                           std::unique_ptr<StandardConfig::ServerNetworkTopologyModel> serverNetworkTopologyModel)
                    : m_valid(true)
                    , m_partitionId(partitionId)
                    , m_replicaId(replicaId)
                    , m_serverNetworkTopologyModel(std::move(serverNetworkTopologyModel))
                    {
                        
                    }
                    
                    bool Valid() const { return m_valid; }
                    
                    Protocols::PartitionIdType PartitionId() const { return m_partitionId; }
                    
                    Protocols::ReplicaIdType ReplicaId() const { return m_replicaId; }
                    
                    const std::string& Host() const { return m_serverNetworkTopologyModel->ReplicatedPartitions()[m_replicaId].HostName(); }
                    
                    const StandardConfig::ServerNetworkTopologyModel& GetServerNetworkTopologyModel() const { return *m_serverNetworkTopologyModel; }
                    
                private:
                    Config()
                    :m_valid(false)
                    , m_partitionId()
                    , m_replicaId()
                    , m_serverNetworkTopologyModel()
                    {
                        
                    }
                    
                    bool m_valid;
                    Protocols::PartitionIdType m_partitionId;
                    Protocols::ReplicaIdType m_replicaId;
                    std::unique_ptr<StandardConfig::ServerNetworkTopologyModel> m_serverNetworkTopologyModel;
                };
            }
        }
    }
}
