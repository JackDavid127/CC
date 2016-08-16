//
//  OrbeDelayedInstallHeartbeatMessageBundleRequestCompleteHandlerProxy.h
//  SimRunner
//
//  Created by Scott on 06/01/2015.
//  Copyright (c) 2015 Scott. All rights reserved.
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
                template<typename TProxiedRequestCompleteHandlerObject>
                class OrbeDelayedInstallHeartbeatMessageBundleRequestCompleteHandlerProxy
                {
                public:
                    OrbeDelayedInstallHeartbeatMessageBundleRequestCompleteHandlerProxy(TProxiedRequestCompleteHandlerObject& proxy)
                    : m_pProxy(&proxy) { }
                    
                    void Finish()
                    {
                        m_pProxy->Finish();
                    }
                    
                private:
                    TProxiedRequestCompleteHandlerObject* m_pProxy;
                };
            }
        }
    }
}
