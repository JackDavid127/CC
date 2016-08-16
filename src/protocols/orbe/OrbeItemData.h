//
//  PackedOrbeItem.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
//
//

#pragma once

#include <string>
#include "Timing.h"
#include "SimRunnerAssert.h"
#include "Orbe.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            template<typename ProtocolTraits>
            struct OrbeItemData
            {
            private:
                typedef typename ProtocolTraits::TStorageKey TKey;
                typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                typedef typename ProtocolTraits::TItemDependencyTimestamp TItemDependencyTimestamp;
                typedef typename ProtocolTraits::TDependencyMatrix TDependencyMatrix;
                
            public:
                TKey Key;
                TClientInputValueType Value;
                TLogicalTimestamp LogicalTimestamp;
                TItemDependencyTimestamp ItemDependencyTimestamp;
                TDependencyMatrix DependencyMatrix;
                Utilities::TTimestamp Timestamp;
                Utilities::TTimestamp ObservedTimestamp;
                ReplicaIdType SourceReplicaId;
            };
        }
    }
}
