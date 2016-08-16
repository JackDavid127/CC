//
//  Protocols.h
//  SimRunner
//
//  Created by Scott on 12/10/2014.
//
//
#pragma once

#include <cstdint>
#include "ECMessaging.h"

namespace SimRunner
{
    namespace Protocols
    {
        typedef int32_t ClientIdType;
        typedef int16_t ReplicaIdType;
        typedef int16_t PartitionIdType;
        
        typedef void* NoContext;
        static constexpr NoContext EmptyContext = nullptr;
    }
}
        