//
//  BoltOnVersionApplier.h
//  SimRunner
//
//  Created by Scott on 15/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <boost/noncopyable.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <deque>
#include <map>
#include <vector>
#include "Protocols.h"
#include "SimRunnerAssert.h"
#include "ApplyAllWritesState.h"
#include "ApplySingleWriteState.h"
#include "CutCoverageAttemptState.h"
//#include "BoltOnMessaging.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            template <typename TProtocolTraits>
            class BoltOnVersionApplier : private boost::noncopyable
            {
                typedef typename TProtocolTraits::TBackingStorage TBackingStorage;
                typedef typename TProtocolTraits::TLocalShimStorage TLocalShimStorage;
                typedef typename TProtocolTraits::TLocalStoreResolver TLocalStoreResolver;
                typedef typename TProtocolTraits::TTaskRunner TTaskRunner;
                typedef typename TProtocolTraits::TStorageKey TStorageKey;
                typedef typename TProtocolTraits::TValueWrapperFactory TValueWrapperFactory;
                typedef typename TProtocolTraits::TValueWrapperPtr TValueWrapperPtr;
                typedef typename TProtocolTraits::TCausalClock TCausalClock;
                typedef typename TProtocolTraits::TBoltOnSerializer TSerializer;
                
                typedef std::map<TStorageKey, std::vector<TCausalClock> > TKeyClockMap;
                typedef std::shared_ptr<TKeyClockMap> TKeyClockMapPtr;
                
                typedef VersionApplierStates::ApplyAllWritesState<TProtocolTraits> TApplyAllWrites;
                typedef VersionApplierStates::ApplySingleWriteState<TProtocolTraits> TApplySingleWrite;
                typedef VersionApplierStates::CutCoverageAttemptState<TProtocolTraits> TCutCoverageAttempt;
                
                // todo -- is it possible to factor out come common components to avoid friending?
                friend TApplyAllWrites;
                friend TApplySingleWrite;
                friend TCutCoverageAttempt;
                
            public:
                BoltOnVersionApplier(TBackingStorage& backingStorage,
                                     TLocalShimStorage& localShimStorage,
                                     TLocalStoreResolver& localStoreResolver,
                                     TSerializer& serializer,
                                     TTaskRunner& taskRunner,
                                     TValueWrapperFactory& valueWrapperFactory)
                : m_backingStorage(backingStorage.Clone())
                , m_localShimStorage(localShimStorage)
                , m_localStoreResolver(localStoreResolver)
                , m_serializer(serializer)
                , m_taskRunner(taskRunner)
                , m_maxBufferedWrites(0)
                , m_limitNumberOfBufferedWrites(false)
                , m_applyAllWritesState(*this, localShimStorage, valueWrapperFactory)
                {
                    
                }
                
                bool HasBufferedWrite(const TStorageKey& key) const
                {
                    return m_bufferedWrites.find(key) != m_bufferedWrites.end();
                }
                
                const TValueWrapperPtr GetBufferedWrite(const TStorageKey& key) const
                {
                    SR_ASSERT(HasBufferedWrite(key));
                    return m_bufferedWrites.find(key)->second;
                }
                
                void AddToCheck(const TValueWrapperPtr valueWrapper)
                {
                    BufferWrite(valueWrapper);
                }
                
                void CheckSingleKey(const TValueWrapperPtr valueWrapper,
                                    size_t currentBackingStoreReadCount,
                                    boost::function<void (bool)> coverageCheckCompletionHandler)
                {
                    TApplySingleWrite::TryApplyWrite(*this,
                                                     m_localShimStorage,
                                                     valueWrapper,
                                                     currentBackingStoreReadCount,
                                                     coverageCheckCompletionHandler);
                }
                
                void TryApplyAllPossible()
                {
                    if(!m_applyAllWritesState.InFlight())
                    {
                        m_applyAllWritesState.TryApply(m_bufferedWrites);
                    }
                }
                
                void DrainEvents()
                {
                    while(!m_asyncApplyEvents.empty())
                    {
                        auto event = m_asyncApplyEvents.front();
                        m_asyncApplyEvents.pop_front();
                        event();
                    }
                }
                
                void ScheduleTask(boost::function<void ()>& task)
                {
                    m_taskRunner.Schedule(task);
                }
                
                void AddToClockConsidered(const TStorageKey& key, const TCausalClock& clock, TKeyClockMapPtr pKeyClockConsiderMap)
                {
                    auto findResult(pKeyClockConsiderMap->find(key));
                    
                    if(findResult == pKeyClockConsiderMap->end())
                    {
                        pKeyClockConsiderMap->insert(std::make_pair(key, std::vector<TCausalClock>()));
                    }
                    else
                    {
                        std::vector<TCausalClock>& clocks(findResult->second);
                        clocks.push_back(clock);
                    }
                }
                
            private:
                
                void AttemptToCover(const TValueWrapperPtr valueWrapper,
                                    std::vector<TValueWrapperPtr>& keysToApply,
                                    size_t currentBackingStoreReadCount,
                                    const boost::function<void(bool)>& completionCallback)
                {
                    TKeyClockMapPtr pKeyClockConsiderMap(new TKeyClockMap);
                    AddToClockConsidered(valueWrapper->Key(), valueWrapper->Clock(), pKeyClockConsiderMap);
                    
                    AttemptToCoverCut(valueWrapper,
                                      keysToApply,
                                      pKeyClockConsiderMap,
                                      currentBackingStoreReadCount,
                                      completionCallback);
                }
                
                void AttemptToCoverCut(const TValueWrapperPtr primaryValueWrapper,
                                       std::vector<TValueWrapperPtr>& keysToApply,
                                       TKeyClockMapPtr pKeyClockConsiderMap,
                                       size_t remainingDepth,
                                       const boost::function<void(bool)>& completionCallback)
                {
                    TCutCoverageAttempt::TryCoverCut(*this,
                                                     m_localShimStorage,
                                                     m_backingStorage->Clone(),
                                                     m_localStoreResolver,
                                                     m_serializer,
                                                     primaryValueWrapper,
                                                     keysToApply,
                                                     pKeyClockConsiderMap,
                                                     remainingDepth,
                                                     completionCallback);
                }
                
                bool CanBufferWrite() const
                {
                    if(m_limitNumberOfBufferedWrites)
                    {
                        return m_bufferedWrites.size() < m_maxBufferedWrites;
                    }
                    else
                    {
                        return true;
                    }
                }
                
                void BufferWrite(const TValueWrapperPtr valueWrapper)
                {
                    if(CanBufferWrite())
                    {
                        m_bufferedWrites.insert(std::make_pair(valueWrapper->Key(), valueWrapper));
                    }
                }
                
                void RemoveBufferedWrite(const TValueWrapperPtr valueWrapper)
                {
                    if(HasBufferedWrite(valueWrapper->Key()))
                    {
                        m_bufferedWrites.erase(valueWrapper->Key());
                    }
                }
                
                void InsertAsyncApplyEvent(const boost::function<void ()>& event)
                {
                    m_asyncApplyEvents.push_back(event);
                }
                
                std::unique_ptr<TBackingStorage> m_backingStorage;
                TLocalShimStorage& m_localShimStorage;
                TLocalStoreResolver& m_localStoreResolver;
                TSerializer& m_serializer;
                TTaskRunner& m_taskRunner;
                
                std::map<TStorageKey, TValueWrapperPtr> m_bufferedWrites;
                std::deque<boost::function<void ()> > m_asyncApplyEvents;
                
                TApplyAllWrites m_applyAllWritesState;
                
                size_t m_maxBufferedWrites;
                bool m_limitNumberOfBufferedWrites;
            };
        }
    }
}
