//
//  OrbeDelayedInstallNetworkCompleteHandlerProxy.h
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

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            namespace SimulatedComponents
            {
                template<typename TProxiedNetworkCompleteHandlerObject>
                class OrbeDelayedInstallNetworkCompleteHandlerProxy
                {
                public:
                    OrbeDelayedInstallNetworkCompleteHandlerProxy(TProxiedNetworkCompleteHandlerObject& proxy)
                    : m_pProxy(&proxy) { }
                    
                    template<typename TRequestMetaData>
                    void HandleLinkOperationComplete(TRequestMetaData& state)
                    {
                        m_pProxy->HandleLinkOperationComplete(state);
                    }
                    
                private:
                    TProxiedNetworkCompleteHandlerObject* m_pProxy;
                };
            }
        }
    }
}
