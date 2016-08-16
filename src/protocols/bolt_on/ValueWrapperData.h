//
//  ValueWrapperData.h
//  SimRunner
//
//  Created by Scott on 18/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//


#pragma once

#include "BoltOn.h"
#include "Timing.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            template<typename TProtocolTraits>
            struct ValueWrapperData
            {
                typedef typename TProtocolTraits::TStorageKey TStorageKey;
                typedef typename TProtocolTraits::TClientInputValueType TClientInputValueType;
                typedef typename TProtocolTraits::TKeyDependencies TKeyDependencies;
                typedef Utilities::TTimestamp TTimestamp;
                
                TStorageKey Key;
                TClientInputValueType Value;
                TKeyDependencies KeyDependencies;
                TTimestamp CreationTimestamp;
                TTimestamp FirstSeenTimestamp;
                TTimestamp RevealedTimestamp;
                
                ValueWrapperData()
                : m_allocated(false)
                , KeyDependencies(TKeyDependencies::CreateEmpty())
                , FirstSeenTimestamp(0)
                , RevealedTimestamp(0)
                { }
                
                ~ValueWrapperData()
                {
                    //SR_ASSERT(false);
                }
                
                void Allocate()
                {
                    SR_ASSERT(!IsAllocated());
                    m_allocated = true;
                    KeyDependencies = TKeyDependencies::CreateEmpty();
                    FirstSeenTimestamp = TTimestamp(0);
                    RevealedTimestamp = TTimestamp(0);
                }
                
                void Allocate(const TStorageKey& key,
                              const TClientInputValueType& value,
                              const TKeyDependencies& keyDependencies,
                              const TTimestamp& creationTimestamp)
                {
                    SR_ASSERT(!IsAllocated());
                    m_allocated = true;
                    Key = key;
                    Value = value;
                    KeyDependencies = keyDependencies;
                    CreationTimestamp = creationTimestamp;
                    FirstSeenTimestamp = TTimestamp(0);
                    RevealedTimestamp = TTimestamp(0);
                }
                
                bool IsAllocated() const
                {
                    return m_allocated == true;
                }
                
                void Release()
                {
                    SR_ASSERT(IsAllocated());
                    m_allocated = false;
                }
                
            private:
                bool m_allocated;
            };
        }
    }
}
