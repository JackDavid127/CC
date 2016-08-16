//
//  DelayStatsOperation.h
//  SimRunner
//
//  Created by Scott on 12/01/2015.
//  Copyright (c) 2015 Scott. All rights reserved.
//

#pragma once

#include "Timing.h"

namespace SimRunner
{
    namespace Metrics
    {
        namespace Delay
        {
            class DelayStatsOperation
            {
                typedef Utilities::TTimeDelta TTimeDelta;
                
            public:
                DelayStatsOperation(size_t numberOfPartitionDependencies, const TTimeDelta& writeVisibilityLatency)
                : m_numberOfPartitionDependencies(numberOfPartitionDependencies)
                , m_writeVisibilityLatency(writeVisibilityLatency)
                {
                    
                }
                
                size_t NumberOfPartitionDependencies() const
                {
                    return m_numberOfPartitionDependencies;
                }
                
                double WriteVisibilityLatency() const
                {
                    return static_cast<double>(m_writeVisibilityLatency.count());
                }
                
            private:
                size_t m_numberOfPartitionDependencies;
                TTimeDelta m_writeVisibilityLatency;
            };
        }
    }
}
