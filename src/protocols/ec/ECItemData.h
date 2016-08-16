//
//  PackedECItem.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
//
//

#pragma once

#include <string>
#include "Timing.h"
#include "SimRunnerAssert.h"
#include "EC.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace EC
        {
            template<typename TKey, typename TClientInputValueType>
            struct ECItemData
            {
                TKey Key;
                TClientInputValueType Value;
                Utilities::TTimestamp Timestamp;
                Utilities::TTimestamp ObservedTimestamp;
                ReplicaIdType SourceReplicaId;
            }; 
        }
    }
}
