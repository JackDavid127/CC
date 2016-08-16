//
//  SimulationClock.h
//  SimRunner
//
//  Created by Scott on 24/09/2014.
//
//

#pragma once

#include "SimulationTime.h"
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace SimulationInfrastructure
    {
        class SimulationClock
        {
        public:
            SimulationClock()
            : m_currentTimeMicroseconds(0)
            {
                
            }
            
            SimulationTimeMicroSeconds CurrentMicroSeconds() const
            {
                return m_currentTimeMicroseconds;
            }
            
            void UpdateTime(SimulationTimeMicroSeconds currentTimeMicroseconds)
            {
                SR_ASSERT(currentTimeMicroseconds >= m_currentTimeMicroseconds);
                m_currentTimeMicroseconds = currentTimeMicroseconds;
            }
            
        private:
             SimulationTimeMicroSeconds m_currentTimeMicroseconds;
        };
    }
}
