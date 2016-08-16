//
//  NetworkOperationStateRequestCompleteHandlerProxy.h
//  SimRunner
//
//  Created by Scott on 01/11/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <memory>
#include <boost/noncopyable.hpp>
#include "Orbe.h"
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            namespace SimulatedComponents
            {
                template<typename TProxiedRequestCompleteHandlerObject>
                class OrbeNetworkOperationStateRequestCompleteHandlerProxy
                {
                public:
                    OrbeNetworkOperationStateRequestCompleteHandlerProxy(TProxiedRequestCompleteHandlerObject& proxy)
                    : m_pProxy(&proxy) { }
                    
                    void Next()
                    {
                        m_pProxy->Next();
                    }
                    
                private:
                    TProxiedRequestCompleteHandlerObject* m_pProxy;
                };
            }
        }
    }
}
