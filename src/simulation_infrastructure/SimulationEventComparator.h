//
//  SimulationEventComparator.h
//  SimRunner
//
//  Created by Scott on 25/09/2014.
//
//

#pragma once

#include "SimulationInfrastructure.h"
#include "SimulationEvent.h"

namespace SimRunner
{
    namespace SimulationInfrastructure
    {
        class SimulationEventComparator
        {
        public:
            bool operator() (const SimulationEvent& lhs, const SimulationEvent& rhs) const
            {
                if(lhs.CompletionTime() == rhs.CompletionTime())
                {
                    return rhs.EventTag() < lhs.EventTag();
                }
                
                return rhs.CompletionTime() < lhs.CompletionTime();
            }
        };
    }
}
