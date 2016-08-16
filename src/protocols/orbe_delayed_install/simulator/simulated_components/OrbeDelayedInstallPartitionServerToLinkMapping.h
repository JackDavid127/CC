//
//  OrbeDelayedInstallPartitionServerToLinkMapping.h
//  SimRunner
//
//  Created by Scott on 01/11/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <memory>
#include <boost/noncopyable.hpp>
#include "OrbeDelayedInstall.h"
#include "SimRunnerAssert.h"
#include "OrbeDelayedInstallNetworkCompleteHandlerProxy.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            namespace SimulatedComponents
            {
                template <typename TPartitionServer, typename TNetworkLink>
                class OrbeDelayedInstallPartitionServerToLinkMapping : private boost::noncopyable
                {
                public:
                    OrbeDelayedInstallPartitionServerToLinkMapping()
                    : m_pPartitionServer(nullptr)
                    , m_pNetworkLink(nullptr)
                    {
                        
                    }
                    
                    bool IsValid() const
                    {
                        return m_pPartitionServer != nullptr;
                    }
                    
                    template <typename TSchedule>
                    void Initialise(TPartitionServer& partitionServer,
                                    TSchedule& schedule)
                    {
                        SR_ASSERT(!IsValid());
                        
                        m_pPartitionServer = &partitionServer;
                        m_pNetworkLink = std::unique_ptr<TNetworkLink>(new TNetworkLink("", schedule));
                    }
                    
                    TPartitionServer& PartitionServer() const
                    {
                        SR_ASSERT(IsValid());
                        return *m_pPartitionServer;
                    }
                    
                    TNetworkLink& Link() const
                    {
                        SR_ASSERT(IsValid());
                        return *m_pNetworkLink;
                    }
                    
                private:
                    TPartitionServer* m_pPartitionServer;
                    std::unique_ptr<TNetworkLink> m_pNetworkLink;
                };
            }
        }
    }
}
