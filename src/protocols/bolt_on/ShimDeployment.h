//
//  ShimDeployment.h
//  SimRunner
//
//  Created by Scott on 15/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//


#pragma once

#include <boost/noncopyable.hpp>
#include <boost/function.hpp>
#include <vector>
#include <set>
#include "BoltOn.h"
#include "AsynchronousReadShimBackEnd.h"
#include "AsynchronousResolver.h"
#include "Timing.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            template<typename TProtocolTraits>
            class ShimDeployment : private boost::noncopyable
            {
                typedef typename TProtocolTraits::TShimId TShimId;
                typedef typename TProtocolTraits::TBackingStorage TBackingStorage;
                typedef typename TProtocolTraits::TLocalShimStorage TLocalShimStorage;
                typedef typename TProtocolTraits::TLocalStoreResolver TLocalStoreResolver;
                typedef typename TProtocolTraits::TShimBackEnd TShimBackEnd;
                typedef typename TProtocolTraits::TStorageKey TStorageKey;
                typedef typename TProtocolTraits::TValueWrapperPtr TValueWrapperPtr;
                typedef typename TProtocolTraits::TClientInputValueType TClientInputValueType;
                typedef typename TProtocolTraits::TTaskRunner TTaskRunner;
                typedef typename TProtocolTraits::TBoltOnSerializer TSerializer;
                typedef SimRunner::Utilities::TTimestamp TTimestamp;

            public:
                ShimDeployment(TShimId& shimDeploymentId,
                               TLocalStoreResolver& resolver,
                               TShimBackEnd& shimBackEnd)
                : m_shimDeploymentId(shimDeploymentId)
                , m_localStoreResolver(resolver)
                , m_shimBackEnd(shimBackEnd)
                {

                }

                const TShimId& DeploymentId() const
                {
                    return m_shimDeploymentId;
                }

                void Get(TBackingStorage& backingStorage,
                         const TStorageKey& key,
                         const boost::function<void (const TValueWrapperPtr)>& getCompleteHandler)
                {
                    m_shimBackEnd.Get(backingStorage, key, getCompleteHandler);
                }

                void Gets(TBackingStorage& backingStorage,
                         const std::vector<TStorageKey>& keys,
                         const boost::function<void (const std::vector<TValueWrapperPtr>)>& getCompleteHandler)
                {
                    m_shimBackEnd.Gets(backingStorage, keys, getCompleteHandler);
                }

                TValueWrapperPtr Put(TBackingStorage& backingStorage,
                                     const TStorageKey& key,
                                     const TClientInputValueType& value,
                                     const boost::function<void (const TValueWrapperPtr)>& putCompleteHandler)
                {
                    return m_shimBackEnd.Put(backingStorage, key, value, putCompleteHandler);
                }

                TValueWrapperPtr PutAfterDependency(TBackingStorage& backingStorage,
                                                    const TStorageKey& key,
                                                    const TClientInputValueType& value,
                                                    const TValueWrapperPtr dependency,
                                                    const boost::function<void (const TValueWrapperPtr)>& putCompleteHandler)
                {
                    return m_shimBackEnd.PutAfterDependency(backingStorage, key, value, dependency, putCompleteHandler);
                }

                TValueWrapperPtr PutAfterDependencies(TBackingStorage& backingStorage,
                                                      const TStorageKey& key,
                                                      const TClientInputValueType& value,
                                                      const std::set<TValueWrapperPtr>& dependencies,
                                                      const boost::function<void (const TValueWrapperPtr)>& putCompleteHandler)
                {
                    return m_shimBackEnd.PutAfterDependencies(backingStorage, key, value, dependencies, putCompleteHandler);
                }

                void UpdateResolver()
                {
                    m_localStoreResolver.Update();
                }

            private:
                TShimId m_shimDeploymentId;
                TLocalStoreResolver& m_localStoreResolver;
                TShimBackEnd& m_shimBackEnd;
            };
        }
    }
}
