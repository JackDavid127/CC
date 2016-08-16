//
//  Orbe.h
//  SimRunner
//
//  Created by Scott on 05/10/2014.
//
//

#pragma once

#include "Protocols.h"
#include "OrbeMessaging.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            template<typename TProtocolTraits>
            class OrbePartitionServer;
            
            template <typename TProtocolTraits>
            class OrbeClient;
            
            template<
            typename TStorage,
            typename TSchedule,
            typename TKeyGenerator,
            typename TKeyToPartitionMapper,
            typename TRandomEngine,
            typename TLogger,
            typename TMetrics,
            typename TDelayTracking,
            typename TClientInputValueType,
            typename TClientGetReplyValueType,
            typename TClientPutReplyValueType,
            typename TProtocolTraits
            >
            class OrbeSimulation;
            
            namespace SimulatedComponents
            {
                template <typename TSchedule, typename TProtocolTraits>
                class OrbeSimulatedExchange;
            }
        }
    }
}
