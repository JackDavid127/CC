//
//  ReplicateMessageBundleRequestCompleteHandlerProxy.h
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
                template<typename TProxiedRequestCompleteHandlerObject>
                class OrbeDelayedInstallReplicateMessageBundleRequestCompleteHandlerProxy
                {
                public:
                    OrbeDelayedInstallReplicateMessageBundleRequestCompleteHandlerProxy(TProxiedRequestCompleteHandlerObject& proxy)
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
