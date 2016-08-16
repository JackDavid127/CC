//
//  ValueWrapper.h
//  SimRunner
//
//  Created by Scott on 15/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "BoltOn.h"
#include "SimRunnerAssert.h"
#include "Timing.h"
#include "ValueWrapperData.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            template<typename TProtocolTraits>
            class ValueWrapper
            {
                typedef typename TProtocolTraits::TStorageKey TStorageKey;
                typedef typename TProtocolTraits::TClientInputValueType TClientInputValueType;
                typedef typename TProtocolTraits::TKeyDependencies TKeyDependencies;
                typedef typename TProtocolTraits::TCausalClock TCausalClock;
                typedef typename TProtocolTraits::TBoltOnSerializedValueTypePtr TBoltOnSerializedValueTypePtr;
                
                typedef Utilities::TTimestamp TTimestamp;
                
            public:
                ValueWrapper()
                : m_allocated(false)
                { }
                
                ~ValueWrapper()
                {
                    //SR_ASSERT(false);
                }
                
                void Allocate(TBoltOnSerializedValueTypePtr data)
                {
                    SR_ASSERT(!IsAllocated());
                    m_allocated = true;
                    m_data = data;
                }
                
                bool IsAllocated() const
                {
                    return m_allocated == true;
                }
                
                void Release()
                {
                    SR_ASSERT(IsAllocated());
                    m_allocated = false;
                    m_data = nullptr;
                }
                
                TBoltOnSerializedValueTypePtr ToData() const
                {
                    SR_ASSERT(IsAllocated());
                    return m_data;
                }
                
                const TStorageKey& Key() const
                {
                    SR_ASSERT(IsAllocated());
                    return m_data->Key;
                }
                
                const TClientInputValueType& Value() const
                {
                    SR_ASSERT(IsAllocated());
                    return m_data->Value;
                }
                
                const TTimestamp& CreationTimestamp() const
                {
                    SR_ASSERT(IsAllocated());
                    return m_data->CreationTimestamp;
                }
                
                const TTimestamp& FirstSeenTimestamp() const
                {
                    SR_ASSERT(IsAllocated());
                    return m_data->FirstSeenTimestamp;
                }
                
                const TTimestamp& RevealedTimestamp() const
                {
                    SR_ASSERT(IsAllocated());
                    SR_ASSERT(Revealed());
                    return m_data->RevealedTimestamp;
                }
                
                const TTimestamp& VisibilityDelaySeconds() const
                {
                    SR_ASSERT(IsAllocated());
                    return RevealedTimestamp() - FirstSeenTimestamp();
                }
                
                bool Revealed() const
                {
                    return m_data->RevealedTimestamp != 0;
                }
                
                const TCausalClock& Clock() const
                {
                    SR_ASSERT(IsAllocated());
                    return GetCausalClockForKey(Key());
                }
                
                const TKeyDependencies& KeyDependencies() const
                {
                    SR_ASSERT(IsAllocated());
                    return m_data->KeyDependencies;
                }
                
                const TCausalClock& GetCausalClockForKey(const TStorageKey& key) const
                {
                    SR_ASSERT(IsAllocated());
                    return KeyDependencies().GetClock(key);
                }
                
                void MarkFirstSeenByShim()
                {
                    SR_ASSERT(IsAllocated());
                    //if(m_data.FirstSeenTimestamp == 0)
                    {
                        m_data->FirstSeenTimestamp = Utilities::Now();
                    }
                }
                
                void RevealToShim()
                {
                    SR_ASSERT(IsAllocated());
                    if(!Revealed())
                    {
                        TTimestamp now(Utilities::Now());
                        
                        if(m_data->FirstSeenTimestamp == 0)
                        {
                            m_data->FirstSeenTimestamp = now;
                        }
                        
                        m_data->RevealedTimestamp = now;
                    }
                }
                
                bool operator<(const ValueWrapper& other) const
                {
                    SR_ASSERT(IsAllocated());
                    return Key() < other.Key();
                }
                
            private:
                bool m_allocated;
                TBoltOnSerializedValueTypePtr m_data;
            };
        }
    }
}
