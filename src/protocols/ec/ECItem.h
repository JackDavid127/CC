//
//  ECItem.h
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
#include "ECItemData.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace EC
        {
            template<typename TKey, typename TClientInputValueType>
            class ECItem
            {
            public:
                ECItem(const TKey& key,
                       const TClientInputValueType& value,
                       const Utilities::TTimestamp& timestamp,
                       ReplicaIdType sourceReplicaId)
                {
                    m_data.Key = key;
                    m_data.Value = value;
                    m_data.Timestamp = timestamp;
                    m_data.SourceReplicaId = sourceReplicaId;
                    m_data.ObservedTimestamp = Utilities::TTimestamp(0);
                }
                
                static ECItem<TKey, TClientInputValueType> FromData(const ECItemData<TKey, TClientInputValueType>& data)
                {
                    ECItem<TKey, TClientInputValueType> item(data.Key,
                                                             data.Value,
                                                             data.Timestamp,
                                                             data.SourceReplicaId);
                    
                    item.m_data.ObservedTimestamp = data.ObservedTimestamp;

                    return item;
                }
                
                ECItemData<TKey, TClientInputValueType> ToData() const
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
                ECItemData<TKey, TClientInputValueType> m_data;
            };
        }
    }
}
