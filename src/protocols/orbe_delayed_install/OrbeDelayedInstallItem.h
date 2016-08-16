//
//  OrbeDelayedInstallItem.h
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
#include "OrbeDelayedInstallItemData.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            template<typename ProtocolTraits>
            class OrbeDelayedInstallItem
            {
                typedef typename ProtocolTraits::TStorageKey TKey;
                typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                typedef typename ProtocolTraits::TItemDependencyTimestamp TItemDependencyTimestamp;
                typedef typename ProtocolTraits::TItemDependencyReplica TItemDependencyReplica;
                typedef typename ProtocolTraits::TClientId TClientId;
                typedef typename ProtocolTraits::TClientDependencyTimestamp TClientDependencyTimestamp;
                typedef typename ProtocolTraits::TClientDependencyPartition TClientDependencyPartition;
                
                typedef OrbeDelayedInstallItem<ProtocolTraits> TSelf;
                typedef OrbeDelayedInstallItemData<ProtocolTraits> TData;
                
            public:
                OrbeDelayedInstallItem(const TKey& key,
                                       const TClientInputValueType& value,
                                       const TLogicalTimestamp& logicalTimestamp,
                                       const TItemDependencyTimestamp& itemDependencyTimestamp,
                                       const TItemDependencyReplica& itemDependencyReplica,
                                       const TClientId& clientId,
                                       const TClientDependencyTimestamp& clientDependencyTimestamp,
                                       const TClientDependencyPartition& clientDependencyPartition,
                                       const Utilities::TTimestamp& timestamp,
                                       ReplicaIdType sourceReplicaId)
                {
                    m_data.Key = key;
                    m_data.Value = value;
                    m_data.LogicalTimestamp = logicalTimestamp;
                    m_data.ItemDependencyTimestamp = itemDependencyTimestamp;
                    m_data.ItemDependencyReplica = itemDependencyReplica;
                    m_data.ClientId = clientId;
                    m_data.ClientDependencyTimestamp = clientDependencyTimestamp;
                    m_data.ClientDependencyPartition = clientDependencyPartition;
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
                               data.ItemDependencyReplica,
                               data.ClientId,
                               data.ClientDependencyTimestamp,
                               data.ClientDependencyPartition,
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
                
                const TItemDependencyReplica& ItemDependencyReplica() const
                {
                    return m_data.ItemDependencyReplica;
                }
                
                const TClientId& ClientId() const
                {
                    return m_data.ClientId;
                }
                
                const TClientDependencyTimestamp& ClientDependencyTimestamp() const
                {
                    return m_data.ClientDependencyTimestamp;
                }
                
                const TClientDependencyPartition& ClientDependencyPartition() const
                {
                    return m_data.ClientDependencyPartition;
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
                
                /*void ConfirmObservable() const
                {
                    SR_ASSERT(!ReplicationObserved());
                    m_data.ObservedTimestamp = Utilities::Now();
                }
                
                Utilities::TTimeDelta ObservationLatency(Utilities::TTimestamp& obervedTimestamp) const
                {
                    SR_ASSERT(ReplicationObserved());
                    return Utilities::GetDeltaTime(m_data.ObservedTimestamp - timestamp);
                }*/
                
            private:
                OrbeDelayedInstallItemData<ProtocolTraits> m_data;
            };
        }
    }
}
