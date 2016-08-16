//
//  OrbeDelayedInstall.h
//  SimRunner
//
//  Created by Scott on 05/10/2014.
//
//

#pragma once

#include "Protocols.h"
#include "OrbeDelayedInstallMessaging.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            template<typename TProtocolTraits>
            class OrbeDelayedInstallPartitionServer;
            
            template <typename TProtocolTraits>
            class OrbeDelayedInstallClient;
            
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
            class OrbeDelayedInstallSimulation;
            
            namespace SimulatedComponents
            {
                template <typename TSchedule, typename TProtocolTraits>
                class OrbeDelayedInstallSimulatedExchange;
            }
        }
    }
}
