//
//  SimulationInfrastructure.h
//  SimRunner
//
//  Created by Scott on 25/09/2014.
//
//

#pragma once

#include "SimulationTime.h"
#include "SimulationEventTag.h"

namespace SimRunner
{
    namespace SimulationInfrastructure
    {
        class SimulationClock;
        class SimulationEvent;
        
        template <typename TSimulationClock, typename TLogger>
        class SimulationEventSchedule;
;
    }
}
