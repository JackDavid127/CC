//
//  ValueWrapperFactory.h
//  SimRunner
//
//  Created by Scott on 21/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "BoltOn.h"
#include "SimRunnerAssert.h"
#include "Pool.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            template<typename TProtocolTraits>
            class ValueWrapperDataFactory
            {
                typedef typename TProtocolTraits::TStorageKey TStorageKey;
                typedef typename TProtocolTraits::TClientInputValueType TClientInputValueType;
                typedef typename TProtocolTraits::TKeyDependencies TKeyDependencies;
                typedef typename TProtocolTraits::TCausalClock TCausalClock;
                typedef typename TProtocolTraits::TValueWrapperData TValueWrapperData;
                typedef typename TProtocolTraits::TBoltOnSerializedValueTypePtr TBoltOnSerializedValueTypePtr;
                
                typedef Utilities::PoolSlab<TValueWrapperData, 1024 * 8> TPoolSlab;
                typedef Utilities::PoolDeleter<TPoolSlab> TPoolDeleter;
                
                typedef Utilities::TTimestamp TTimestamp;
                
            public:
                ValueWrapperDataFactory()
                : m_pool("value data")
                , m_deleter(m_pool)
                {
                    
                }
                
                TBoltOnSerializedValueTypePtr Create()
                {
                    TBoltOnSerializedValueTypePtr pValue(CreateInner());
                    pValue->Allocate();
                    return pValue;
                }
                
                TBoltOnSerializedValueTypePtr Create(const TStorageKey& key,
                                                     const TClientInputValueType& value,
                                                     const TKeyDependencies& keyDependencies,
                                                     const TTimestamp& creationTimestamp)
                {
                    TBoltOnSerializedValueTypePtr pValue(CreateInner());
                    pValue->Allocate(key, value, keyDependencies, creationTimestamp);
                    return pValue;
                }
                
            private:
                TBoltOnSerializedValueTypePtr CreateInner()
                {
                    SR_ASSERT(m_pool.HasFreeEntryAvailable());
                    TValueWrapperData* pValue(m_pool.Allocate());
                    return TBoltOnSerializedValueTypePtr(pValue, m_deleter);
                }
                
                TPoolSlab m_pool;
                TPoolDeleter m_deleter;
            };
            
            template<typename TProtocolTraits>
            class ValueWrapperFactory
            {
                typedef typename TProtocolTraits::TStorageKey TStorageKey;
                typedef typename TProtocolTraits::TClientInputValueType TClientInputValueType;
                typedef typename TProtocolTraits::TKeyDependencies TKeyDependencies;
                typedef typename TProtocolTraits::TCausalClock TCausalClock;
                typedef typename TProtocolTraits::TValueWrapper TValueWrapper;
                typedef typename TProtocolTraits::TValueWrapperPtr TValueWrapperPtr;
                typedef typename TProtocolTraits::TValueWrapperDataFactory TValueWrapperDataFactory;
                typedef typename TProtocolTraits::TBoltOnSerializedValueTypePtr TBoltOnSerializedValueTypePtr;
                
                typedef Utilities::PoolSlab<TValueWrapper, 1024 * 8> TPoolSlab;
                typedef Utilities::PoolDeleter<TPoolSlab> TPoolDeleter;
                
                typedef Utilities::TTimestamp TTimestamp;
                
            public:
                ValueWrapperFactory(TValueWrapperDataFactory& valueWrapperDataFactory)
                : m_pool("value wrapper")
                , m_deleter(m_pool)
                , m_valueWrapperDataFactory(valueWrapperDataFactory)
                {
                    
                }
                
                TValueWrapperPtr CreateValueWrapper(const TStorageKey& key,
                                                    const TClientInputValueType& value,
                                                    const TKeyDependencies& keyDependencies,
                                                    const TTimestamp& creationTimestamp)
                {
                    SR_ASSERT(m_pool.HasFreeEntryAvailable());
                    TValueWrapper* pValue(m_pool.Allocate());
                    TBoltOnSerializedValueTypePtr pData(m_valueWrapperDataFactory.Create(key, value, keyDependencies, creationTimestamp));
                    pValue->Allocate(pData);
                    return TValueWrapperPtr(pValue, m_deleter);
                }
                
                TValueWrapperPtr CreateValueWrapper(const TBoltOnSerializedValueTypePtr data)
                {
                    SR_ASSERT(m_pool.HasFreeEntryAvailable());
                    TValueWrapper* pValue(m_pool.Allocate());
                    pValue->Allocate(data);
                    return TValueWrapperPtr(pValue, m_deleter);
                }
                
            private:
                TValueWrapperDataFactory& m_valueWrapperDataFactory;
                TPoolSlab m_pool;
                TPoolDeleter m_deleter;
            };
        }
    }
}
