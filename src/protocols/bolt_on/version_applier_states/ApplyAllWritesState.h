//
//  ApplyAllWritesState.h
//  SimRunner
//
//  Created by Scott on 18/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <boost/noncopyable.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <deque>
#include <map>
#include <vector>
#include <limits>
#include "Protocols.h"
#include "SimRunnerAssert.h"
//#include "BoltOnMessaging.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            namespace VersionApplierStates
            {
                template <typename TProtocolTraits>
                class ApplyAllWritesState : private boost::noncopyable
                {
                    typedef typename TProtocolTraits::TBackingStorage TBackingStorage;
                    typedef typename TProtocolTraits::TLocalShimStorage TLocalShimStorage;
                    typedef typename TProtocolTraits::TLocalStoreResolver TLocalStoreResolver;
                    typedef typename TProtocolTraits::TTaskRunner TTaskRunner;
                    typedef typename TProtocolTraits::TStorageKey TStorageKey;
                    typedef typename TProtocolTraits::TValueWrapperPtr TValueWrapperPtr;
                    typedef typename TProtocolTraits::TValueWrapperFactory TValueWrapperFactory;
                    typedef typename TProtocolTraits::TCausalClock TCausalClock;
                    typedef typename TProtocolTraits::TVersionApplier TVersionApplier;
                    
                public:
                    ApplyAllWritesState(TVersionApplier& versionApplier,
                                        TLocalShimStorage& localShimStorage,
                                        TValueWrapperFactory& valueWrapperFactory)
                    : m_versionApplier(versionApplier)
                    , m_localShimStorage(localShimStorage)
                    , m_inFlight(false)
                    , m_currentProcessingWriteIndex(0)
                    , m_coverageAttemptCompletionCallback(boost::bind(&ApplyAllWritesState::HandleCoverageAttemptCompleted, this, _1))
                    {
                        
                    }
                    
                    bool InFlight() const
                    {
                        return m_inFlight;
                    }
                    
                    void TryApply(const std::map<TStorageKey, TValueWrapperPtr>& bufferedWrites)
                    {
                        SR_ASSERT(!m_inFlight);
                        m_inFlight = true;
                        m_bufferedWrites = bufferedWrites;
                        m_currentProcessingWriteIndex = 0;
                        m_keysToApply.clear();
                    }
                    
                private:
                    void ProcessNextWrite()
                    {
                        SR_ASSERT(m_inFlight);
                        
                        if(m_currentProcessingWriteIndex == m_bufferedWrites.size())
                        {
                            m_inFlight = false;
                        }
                        else
                        {
                            ++ m_currentProcessingWriteIndex;
                            m_keysToApply.clear();
                            
                            TValueWrapperPtr frontierWrite(m_bufferedWrites.find(0)->second);
                            
                            m_versionApplier.AttemptToCover(frontierWrite,
                                                            m_keysToApply,
                                                            std::numeric_limits<size_t>::max(),
                                                            m_coverageAttemptCompletionCallback);
                        }
                    }
                    
                    void HandleCoverageAttemptCompleted(bool successfullyCovered)
                    {
                        if(successfullyCovered)
                        {
                            for(auto it = m_keysToApply.begin(); it != m_keysToApply.end(); ++ it)
                            {
                                const TValueWrapperPtr& valueWrapper(*it);
                                m_localShimStorage.Put(valueWrapper->Key(), valueWrapper);
                                m_versionApplier.RemoveBufferedWrite(valueWrapper);
                            }
                        }
                        
                        m_versionApplier.InsertAsyncApplyEvent(boost::bind(&ApplyAllWritesState::ProcessNextWrite, this));
                    }
                    
                private:
                    TVersionApplier& m_versionApplier;
                    TLocalShimStorage& m_localShimStorage;
                    boost::function<void(bool)> m_coverageAttemptCompletionCallback;
                    
                    std::map<TStorageKey, TValueWrapperPtr> m_bufferedWrites;
                    bool m_inFlight;
                    size_t m_currentProcessingWriteIndex;
                    std::vector<TValueWrapperPtr> m_keysToApply;
                };
            }
        }
    }
}
