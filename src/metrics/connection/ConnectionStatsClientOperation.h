//
//  ConnectionStatsClientOperation.h
//  SimRunner
//
//  Created by Scott on 25/11/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "Timing.h"

namespace SimRunner
{
    namespace Metrics
    {
        namespace Connection
        {
            class ConnectionStatsClientOperation
            {
                typedef Utilities::TTimeDelta TTimeDelta;

            public:
                ConnectionStatsClientOperation(bool isGet, TTimeDelta duration)
                : m_requestIsGet(isGet)
                , m_requestDuration(duration)
                {
                    
                }
                
                bool RequestIsGet() const
                {
                    return m_requestIsGet;
                }
                
                double RequestDurationSeconds() const
                {
                    return static_cast<double>(m_requestDuration.count());
                }
                
            private:
                bool m_requestIsGet;
                TTimeDelta m_requestDuration;
            };
        }
    }
}
