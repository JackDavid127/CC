//
//  EC.h
//  SimRunner
//
//  Created by Scott on 05/10/2014.
//
//

#pragma once

#include "Protocols.h"
#include "ECMessaging.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace EC
        {
            template<
            typename TKey,
            typename TSerializedValueType,
            typename TDeserializedValueType,
            typename TClientInputValueType,
            typename TClientGetReplyValueType,
            typename TClientPutReplyValueType,
            typename TReplicationMessageType,
            typename TStorage,
            typename TMetrics,
            typename TDelayTracking,
            typename TLogger,
            typename TProtocolTraits
            >
            class ECPartitionServer;
            
            template <
            typename TKey,
            typename TClientInputValueType,
            typename TClientGetReplyValueType,
            typename TClientPutReplyValueType,
            typename TKeyToPartitionMapper,
            typename TMetrics,
            typename TLogger,
            typename TProtocolTraits
            >
            class ECClient;
            
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
            class ECSimulation;
            
            namespace SimulatedComponents
            {
                template <
                typename TSchedule,
                typename TMetrics,
                typename TLogger,
                typename TKey,
                typename TSerializedValueType,
                typename TDeserializedValueType,
                typename TClientInputValueType,
                typename TClientGetReplyValueType,
                typename TClientPutReplyValueType,
                typename TReplicationMessageType,
                typename TProtocolTraits
                >
                class ECSimulatedExchange;
            }
        }
    }
}
