//
//  PackedOrbeDelayedInstallItem.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
//
//

#pragma once

#include <string>
#include "Timing.h"
#include "SimRunnerAssert.h"
#include "OrbeDelayedInstall.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            template<typename ProtocolTraits>
            struct OrbeDelayedInstallItemData
            {
            private:
                typedef typename ProtocolTraits::TStorageKey TKey;
                typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                typedef typename ProtocolTraits::TItemDependencyTimestamp TItemDependencyTimestamp;
                typedef typename ProtocolTraits::TItemDependencyReplica TItemDependencyReplica;
                typedef typename ProtocolTraits::TClientId TClientId;
                typedef typename ProtocolTraits::TClientDependencyTimestamp TClientDependencyTimestamp;
                typedef typename ProtocolTraits::TClientDependencyPartition TClientDependencyPartition;
                
            public:
                TKey Key;
                TClientInputValueType Value;
                TLogicalTimestamp LogicalTimestamp;
                TItemDependencyTimestamp ItemDependencyTimestamp;
                TItemDependencyReplica ItemDependencyReplica;
                TClientId ClientId;
                TClientDependencyTimestamp ClientDependencyTimestamp;
                TClientDependencyPartition ClientDependencyPartition;
                Utilities::TTimestamp Timestamp;
                Utilities::TTimestamp ObservedTimestamp;
                ReplicaIdType SourceReplicaId;
            };
        }
    }
}
