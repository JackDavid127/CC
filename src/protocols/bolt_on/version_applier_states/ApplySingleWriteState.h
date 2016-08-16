//
//  ApplySingleWriteState.h
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

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            namespace VersionApplierStates
            {
                template <typename TProtocolTraits>
                class ApplySingleWriteState
                : private boost::noncopyable
                {
                    typedef typename TProtocolTraits::TBackingStorage TBackingStorage;
                    typedef typename TProtocolTraits::TLocalShimStorage TLocalShimStorage;
                    typedef typename TProtocolTraits::TLocalStoreResolver TLocalStoreResolver;
                    typedef typename TProtocolTraits::TTaskRunner TTaskRunner;
                    typedef typename TProtocolTraits::TStorageKey TStorageKey;
                    typedef typename TProtocolTraits::TCausalClock TCausalClock;
                    typedef typename TProtocolTraits::TVersionApplier TVersionApplier;
                    typedef typename TProtocolTraits::TValueWrapperPtr TValueWrapperPtr;
                    
                    typedef ApplySingleWriteState<TProtocolTraits> TSelf;
                    typedef TSelf* TSelfPtr;
                    
                public:
                    static void TryApplyWrite(TVersionApplier& versionApplier,
                                              TLocalShimStorage& localShimStorage,
                                              const TValueWrapperPtr frontierWrite,
                                              size_t currentBackingStoreReadCount,
                                              const boost::function<void(bool)>& clientCompletionCallback)
                    {
                        new TSelf(frontierWrite,
                                  versionApplier,
                                  localShimStorage,
                                  currentBackingStoreReadCount,
                                  clientCompletionCallback);
                    }
                    
                    ~ApplySingleWriteState()
                    {
                        
                    }
                    
                private:
                    ApplySingleWriteState(const TValueWrapperPtr frontierWrite,
                                          TVersionApplier& versionApplier,
                                          TLocalShimStorage& localShimStorage,
                                          size_t currentBackingStoreReadCount,
                                          const boost::function<void(bool)>& clientCompletionCallback)
                    : m_versionApplier(versionApplier)
                    , m_localShimStorage(localShimStorage)
                    , m_currentBackingStoreReadCount(currentBackingStoreReadCount)
                    , m_clientCompletionCallback(clientCompletionCallback)
                    {   
                        m_coverageAttemptCompletionCallback = boost::bind(&ApplySingleWriteState::HandleCoverageAttemptCompleted,
                                                                          this,
                                                                          _1);
                        m_versionApplier.AttemptToCover(frontierWrite,
                                                        m_keysToApply,
                                                        m_currentBackingStoreReadCount,
                                                        m_coverageAttemptCompletionCallback);
                    }
                    
                    void HandleCoverageAttemptCompleted(bool successfullyCovered)
                    {
                        if(successfullyCovered)
                        {
                            for(auto it = m_keysToApply.begin(); it != m_keysToApply.end(); ++ it)
                            {
                                const TValueWrapperPtr valueWrapper(*it);
                                m_localShimStorage.Put(valueWrapper->Key(), valueWrapper);
                                m_versionApplier.RemoveBufferedWrite(valueWrapper);
                            }
                        }
                        
                        m_clientCompletionCallback(successfullyCovered);
                        delete this;
                    }
                    
                private:
                    TVersionApplier& m_versionApplier;
                    TLocalShimStorage& m_localShimStorage;
                    size_t m_currentBackingStoreReadCount;
                    boost::function<void(bool)> m_coverageAttemptCompletionCallback;
                    boost::function<void(bool)> m_clientCompletionCallback;
                    
                    std::vector<TValueWrapperPtr> m_keysToApply;
                };
            }
        }
    }
}
