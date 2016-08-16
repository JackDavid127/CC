//
//  OrbeItem.h
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
#include "OrbeItemData.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            template<typename ProtocolTraits>
            class OrbeItem
            {
                typedef typename ProtocolTraits::TStorageKey TKey;
                typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                typedef typename ProtocolTraits::TItemDependencyTimestamp TItemDependencyTimestamp;
                typedef typename ProtocolTraits::TDependencyMatrix TDependencyMatrix;
                
                typedef OrbeItem<ProtocolTraits> TSelf;
                typedef OrbeItemData<ProtocolTraits> TData;
                
            public:
                OrbeItem(const TKey& key,
                         const TClientInputValueType& value,
                         const TLogicalTimestamp& logicalTimestamp,
                         const TItemDependencyTimestamp& itemDependencyTimestamp,
                         const TDependencyMatrix& dependencyMatrix,
                         const Utilities::TTimestamp& timestamp,
                         ReplicaIdType sourceReplicaId)
                {
                    m_data.Key = key;
                    m_data.Value = value;
                    m_data.LogicalTimestamp = logicalTimestamp;
                    m_data.ItemDependencyTimestamp = itemDependencyTimestamp;
                    m_data.DependencyMatrix = dependencyMatrix;
                    m_data.Timestamp = timestamp;
                    m_data.SourceReplicaId = sourceReplicaId;
                    m_data.ObservedTimestamp = Utilities::TTimestamp(0);
                }
                
                static TSelf FromData(const TData& data)
                {
                    TSelf item(data.Key,
                               data.Value,
                               data.LogicalTimestamp,
                               data.ItemDependencyTimestamp,
                               data.DependencyMatrix,
                               data.Timestamp,
                               data.SourceReplicaId);
                    
                    item.m_data.ObservedTimestamp = data.ObservedTimestamp;
                    
                    return item;
                }
                
                TData ToData() const
                {
                    return m_data;
                }
                
                const TKey& Key() const
                {
                    return m_data.Key;
                }
                
                const TClientInputValueType& Value() const
                {
                    return m_data.Value;
                }
                
                const TLogicalTimestamp& LogicalTimestamp() const
                {
                    return m_data.LogicalTimestamp;
                }
                
                const TItemDependencyTimestamp& ItemDependencyTimestamp() const
                {
                    return m_data.ItemDependencyTimestamp;
                }
                
                const TDependencyMatrix& DependencyMatrix() const
                {
                    return m_data.DependencyMatrix;
                }
                
                const Utilities::TTimestamp& Timestamp() const
                {
                    return m_data.Timestamp;
                }
                
                ReplicaIdType SourceReplicaId() const
                {
                    return m_data.SourceReplicaId;
                }
                
                size_t MetaDataSizeBytes() const
                {
                    return 0;
                }
                
                bool ReplicationObserved() const
                {
                    return m_data.ObservedTimestamp.count() != 0;
                }
                
                void ConfirmObservation()
                {
                    SR_ASSERT(!ReplicationObserved());
                    m_data.ObservedTimestamp = Utilities::Now();
                }
                
                Utilities::TTimeDelta ObservationLatency() const
                {
                    SR_ASSERT(ReplicationObserved());
                    return Utilities::GetDeltaTime(m_data.ObservedTimestamp - Timestamp());
                }
                
            private:
                OrbeItemData<ProtocolTraits> m_data;
            };
        }
    }
}
