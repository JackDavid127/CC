//
//  CutCoverageAttemptState.h
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
#include "CurrentKeyProcessState.h"
#include "CausalRelationship.h"

//#define SR_DEBUG_CutCoverageAttemptState_TrackRecurse
//#define SR_DEBUG_CutCoverageAttemptState_LOGGING

#if defined(SR_DEBUG_CutCoverageAttemptState_TrackRecurse)
#include <deque>
#endif

#if defined (SR_DEBUG_CutCoverageAttemptState_LOGGING)
#define SR_DEBUG_CutCoverageAttemptState_LOG(x, ...) { if(x) printf(__VA_ARGS__); }
#else
#define SR_DEBUG_CutCoverageAttemptState_LOG(x, ...) ((void)(x))
#endif

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            namespace VersionApplierStates
            {
                template <typename TProtocolTraits>
                class CutCoverageAttemptState
                : private boost::noncopyable
                {
                    typedef typename TProtocolTraits::TBackingStorage TBackingStorage;
                    typedef typename TProtocolTraits::TLocalShimStorage TLocalShimStorage;
                    typedef typename TProtocolTraits::TLocalStoreResolver TLocalStoreResolver;
                    typedef typename TProtocolTraits::TTaskRunner TTaskRunner;
                    typedef typename TProtocolTraits::TStorageKey TStorageKey;
                    typedef typename TProtocolTraits::TValueWrapperFactory TValueWrapperFactory;
                    typedef typename TProtocolTraits::TValueWrapperPtr TValueWrapperPtr;
                    typedef typename TProtocolTraits::TCausalClock TCausalClock;
                    typedef typename TProtocolTraits::TVersionApplier TVersionApplier;
                    typedef typename TProtocolTraits::TBoltOnSerializer TSerializer;
                    typedef typename TProtocolTraits::TBoltOnSerializedValueTypePtr TBoltOnSerializedValueTypePtr;
                    
                    typedef typename TVersionApplier::TKeyClockMapPtr TKeyClockMapPtr;
                    
                    typedef typename TProtocolTraits::TShimId TShimId;
                    typedef typename TProtocolTraits::TCounter TCounter;
                    
                    typedef CutCoverageAttemptState<TProtocolTraits> TSelf;
                    typedef TSelf* TSelfPtr;
                    typedef CurrentKeyProcessState<TProtocolTraits> TCurrentKeyProcessState;
                    typedef std::shared_ptr<TCurrentKeyProcessState> TCurrentKeyProcessStatePtr;
                    
#if defined(SR_DEBUG_CutCoverageAttemptState_TrackRecurse)
                    std::deque<int>* m_recursionStack;
#endif
                    
                public:
                    static void TryCoverCut(TVersionApplier& versionApplier,
                                            TLocalShimStorage& localShimStorage,
                                            std::unique_ptr<TBackingStorage> backingStorage,
                                            TLocalStoreResolver& localStoreResolver,
                                            TSerializer& serializer,
                                            const TValueWrapperPtr primaryValueWrapper,
                                            std::vector<TValueWrapperPtr>& keysToApply,
                                            TKeyClockMapPtr pKeyClockConsiderMap,
                                            size_t remainingDepth,
                                            const boost::function<void(bool)>& clientCompletionCallback
                                            
#if defined(SR_DEBUG_CutCoverageAttemptState_TrackRecurse)
                                            ,std::deque<int>* recursionStack=nullptr
#endif
                    )
                    {
                        new TSelf(versionApplier,
                                  localShimStorage,
                                  backingStorage->Clone(),
                                  localStoreResolver,
                                  serializer,
                                  primaryValueWrapper,
                                  keysToApply,
                                  pKeyClockConsiderMap,
                                  remainingDepth,
                                  clientCompletionCallback
                                  
#if defined(SR_DEBUG_CutCoverageAttemptState_TrackRecurse)
                                  ,recursionStack == nullptr ? new std::deque<int>() : recursionStack
#endif
                                  );
                    }
                    
                    ~CutCoverageAttemptState()
                    {
                        
#if defined(SR_DEBUG_CutCoverageAttemptState_TrackRecurse)
                        m_recursionStack->pop_front();
                        if(m_recursionStack->size() == 0)
                        {
                            delete m_recursionStack;
                        }
#endif
                    }
                    
                private:
                    CutCoverageAttemptState(TVersionApplier& versionApplier,
                                            TLocalShimStorage& localShimStorage,
                                            std::unique_ptr<TBackingStorage> backingStorage,
                                            TLocalStoreResolver& localStoreResolver,
                                            TSerializer& serializer,
                                            const TValueWrapperPtr primaryValueWrapper,
                                            std::vector<TValueWrapperPtr>& keysToApply,
                                            TKeyClockMapPtr pKeyClockConsiderMap,
                                            size_t remainingDepth,
                                            const boost::function<void(bool)>& clientCompletionCallback
#if defined(SR_DEBUG_CutCoverageAttemptState_TrackRecurse)
                                            ,std::deque<int>* recursionStack
#endif
                    )
                    : m_versionApplier(versionApplier)
                    , m_localShimStorage(localShimStorage)
                    , m_backingStorage(std::move(backingStorage))
                    , m_localStoreResolver(localStoreResolver)
                    , m_serializer(serializer)
                    , m_primaryValueWrapper(primaryValueWrapper)
                    , m_keysToApply(keysToApply)
                    , m_pKeyClockConsiderMap(pKeyClockConsiderMap)
                    , m_remainingDepth(remainingDepth)
                    , m_clientCompletionCallback(clientCompletionCallback)
                    , m_hasCheckedDependencyResultApplied(false)
                    , m_dependencyKeyListCurrent(0)
                    , m_pCurrentKeyProcessingState(nullptr)
                    , m_hasCompleted(false)
                    , m_result(false)
                    , m_logging(false)
                    {
#if defined(SR_DEBUG_CutCoverageAttemptState_TrackRecurse)
                        printf("CutCoverageAttemptStateInstanceCount: %lu, key: %d\n",
                               recursionStack->size(),
                               m_primaryValueWrapper.Key());
                        
                        for(auto it = recursionStack->begin(); it != recursionStack->end(); ++ it)
                        {
                            if(*it == m_primaryValueWrapper.Key())
                            {
                                printf("CYCLE DETECTED -- POTENTIAL FAILURE IMMINENT!\n");
                                m_logging = true;
                                break;
                            }
                        }
                        
                        m_recursionStack = recursionStack;
                        m_recursionStack->push_front(m_primaryValueWrapper.Key());
#endif
                        
                        SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "attempt to cover key --> %d\n", primaryValueWrapper.Key());
                        
                        m_dependencyKeys = m_primaryValueWrapper->KeyDependencies().DependencyKeys();
                        m_runUpdate = boost::bind(&TSelf::Update, this);
                        m_scheduleNextUpdate = boost::bind(&TSelf::ScheduleNextUpdate, this);
                        m_versionApplier.ScheduleTask(m_runUpdate);
                    }
                    
                    bool Result() const
                    {
                        SR_ASSERT(m_hasCompleted);
                        return m_result;
                    }
                    
                    void SetResult(bool result)
                    {
                        SR_ASSERT(!m_hasCompleted);
                        m_hasCompleted = true;
                        m_result = result;
                        m_clientCompletionCallback(Result());
                        delete this; //todo? -- kinda not so nice (some kind of enable shared from this?)
                    }
                    
                    void GetNextDependencyKey()
                    {
                        SR_ASSERT(StillHasKeysToCheck());
                        const TStorageKey& key = m_dependencyKeys[m_dependencyKeyListCurrent];
                        
                        SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "moving on to check dependency key --> %d\n", key);
                        m_pCurrentKeyProcessingState = TCurrentKeyProcessStatePtr(new TCurrentKeyProcessState(key, m_scheduleNextUpdate));
                        ++ m_dependencyKeyListCurrent;
                    }
                    
                    bool StillHasKeysToCheck() const
                    {
                        return m_dependencyKeyListCurrent < m_dependencyKeys.size();
                    }
                    
                    void Update()
                    {
                        if(!m_hasCheckedDependencyResultApplied)
                        {
                            SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "Update :: performing check dependency result applied for key --> %d\n", m_primaryValueWrapper.Key());
                            
                            m_hasCheckedDependencyResultApplied = true;
                            
                            // is this dependency applied already?
                            TValueWrapperPtr* pResult;
                            bool keyResultExists = m_localShimStorage.TryGetValue(m_primaryValueWrapper->Key(), pResult);
                            
                            if(keyResultExists)
                            {
                                SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "result already exists for key %d...\n", m_primaryValueWrapper.Key());
                                //if the value in our local store happens after then we have nothing to do, it is applied already
                                auto causality = (*pResult)->Clock().Compare(m_primaryValueWrapper->Clock());
                                
                                if(causality == CausalRelationship::HappensAfter || causality == CausalRelationship::Equal)
                                {
                                    SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "existing result is newer and should be retained! - finished\n");
                                    SetResult(true);
                                    return;
                                }
                                else
                                {
                                    SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "existing result is old and can be replaced\n");
                                }
                            }
                        }
                        
                        // the key processing will return a result if they terminate this loop -- else, we will process all the keys
                        // and enter the else branch...
                        if(StillHasKeysToCheck())
                        {
                            if(m_pCurrentKeyProcessingState == nullptr || m_pCurrentKeyProcessingState->IsFinished())
                            {
                                GetNextDependencyKey();
                                SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "begin processing dependency key %d\n", m_pCurrentKeyProcessingState->Key);
                                ProcessKey();
                            }
                        }
                        else
                        {
                            SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "done processing key %d - finished\n", m_primaryValueWrapper.Key());
                            m_keysToApply.push_back(m_primaryValueWrapper);
                            SetResult(true);
                        }
                    }
                    
                    void ScheduleNextUpdate()
                    {
                        m_versionApplier.ScheduleTask(m_runUpdate);
                    }
                    
                    void DEBUG_PrintClock(const std::map<TShimId, TCounter>& clockData)
                    {
                        SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "\tCausal Clock:\n");
                        for(auto it = clockData.begin(); it != clockData.end(); ++it)
                        {
                            SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "\t\t%lu -> %lu\n", it->first, it->second.Value());
                        }
                    }
                    
                    void ProcessKey()
                    {
                        SR_ASSERT(m_pCurrentKeyProcessingState != nullptr);
                        
                        const TStorageKey& key = m_pCurrentKeyProcessingState->Key;
                        TCausalClock dependencyClock = m_primaryValueWrapper->GetCausalClockForKey(key);
                        
                        SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "dep clock for key %d\n", key);
                        DEBUG_PrintClock(dependencyClock.Data());
                        
                        if(!m_pCurrentKeyProcessingState->PerformedComparisonToSelf)
                        {
                            m_pCurrentKeyProcessingState->PerformedComparisonToSelf = true;
                            
                            if(key == m_primaryValueWrapper->Key())
                            {
                                SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "dep key %d is self -- finished\n", key);
                                m_pCurrentKeyProcessingState->Finish();
                                return;
                            }
                        }
                        
                        if(!m_pCurrentKeyProcessingState->PerformedCheckForDependencyAlreadySatisfied)
                        {
                            m_pCurrentKeyProcessingState->PerformedCheckForDependencyAlreadySatisfied = true;
                            
                            TValueWrapperPtr* pResult;
                            bool keyResultExists = m_localShimStorage.TryGetValue(m_primaryValueWrapper->Key(), pResult);
                            
                            // is this write applied, or is a later write which would overwrite this already applied?
                            // if so, we are satisfied for this dependency...
                            if(keyResultExists)
                            {
                                if((*pResult)->Clock().Compare(dependencyClock) != CausalRelationship::HappensBefore)
                                {
                                    SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "dep key %d is applied (or a later overwriting write is) -- finished\n", key);
                                    AddToConsideredClocks(key, dependencyClock);
                                    m_pCurrentKeyProcessingState->Finish();
                                    return;
                                }
                            }
                            
                            // have we already checked for a cover for this?
                            if(m_pKeyClockConsiderMap->find(key) != m_pKeyClockConsiderMap->end())
                            {
                                bool found = false;
                                std::vector<TCausalClock>& clocks(m_pKeyClockConsiderMap->find(key)->second);
                                
                                SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "testing %lu additional write clocks\n", clocks.size());
                                SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "clock for key %d:\n", key);
                                DEBUG_PrintClock(dependencyClock.Data());
                                
                                for(auto it = clocks.begin(); it != clocks.end(); ++ it)
                                {
                                    TCausalClock appliedClock(*it);
                                    
                                    DEBUG_PrintClock(appliedClock.Data());
                                    
                                    const auto compareResult = appliedClock.Compare(dependencyClock);
                                    SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "Clock compare result == %d!\n", compareResult);
                                    
                                    if(compareResult != CausalRelationship::HappensBefore)
                                    {
                                        found = true;
                                        break;
                                    }
                                }
                                
                                if(found)
                                {
                                    SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "dep key %d has already been examined -- finished\n", key);
                                    m_pCurrentKeyProcessingState->Finish();
                                    return;
                                }
                            }
                        }
                        
                        if(!m_pCurrentKeyProcessingState->PerformedCheckAgainstBufferedWrites)
                        {
                            m_pCurrentKeyProcessingState->PerformedCheckAgainstBufferedWrites = true;
                            
                            // now check buffered writes
                            if(m_versionApplier.HasBufferedWrite(key))
                            {
                                const TValueWrapperPtr bufferedWrite = m_versionApplier.GetBufferedWrite(key);
                                auto causality = bufferedWrite->Clock().Compare(dependencyClock);
                                
                                bool shouldAdd = false;
                                
                                if(causality == CausalRelationship::Equal)
                                {
                                    AddToConsidered(bufferedWrite, key, dependencyClock);
                                    
                                    shouldAdd = true;
                                }
                                else if(causality == CausalRelationship::HappensAfter || causality == CausalRelationship::Concurrent)
                                {
                                    AddToConsidered(bufferedWrite, key, dependencyClock);
                                    
                                    m_pCurrentKeyProcessingState->HaltedOnTestBufferedWritesCovered();
                                    
                                    boost::function<void(bool)> recursionComplete = boost::bind(&TSelf::CheckWriteRecursionCompletionHandler,
                                                                                                this,
                                                                                                this,
                                                                                                m_pCurrentKeyProcessingState,
                                                                                                key,
                                                                                                bufferedWrite,
                                                                                                dependencyClock,
                                                                                                false,
                                                                                                _1);
                                    
                                    SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "dep key %d is in current buffered set but with a future write -- attempt recursive cover!\n", key);
                                    
                                    SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "clock for buffer read for key %d:\n", key);
                                    DEBUG_PrintClock(bufferedWrite->Clock().Data());
                                    SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "...while dependencyClock for same key is:\n");
                                    DEBUG_PrintClock(dependencyClock.Data());
                                    
                                    TSelf::TryCoverCut(m_versionApplier,
                                                       m_localShimStorage,
                                                       m_backingStorage->Clone(),
                                                       m_localStoreResolver,
                                                       m_serializer,
                                                       bufferedWrite,
                                                       m_keysToApply,
                                                       m_pKeyClockConsiderMap,
                                                       m_remainingDepth,
                                                       recursionComplete
#if defined(SR_DEBUG_CutCoverageAttemptState_TrackRecurse)
                                                       ,m_recursionStack
#endif
                                                       );
                                    
                                    return;
                                }
                                
                                if(shouldAdd)
                                {
                                    SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "dep key %d is in current buffered set -- finished\n", key);
                                    m_pCurrentKeyProcessingState->Finish();
                                    return;
                                }
                            }
                        }
                        
                        if(!m_pCurrentKeyProcessingState->PerformedReadBackingStore)
                        {
                            m_pCurrentKeyProcessingState->PerformedReadBackingStore = true;
                            
                            boost::function<void (const TBoltOnSerializedValueTypePtr)> backingStoreReadComplete
                            = boost::bind(&TSelf::HandleReceivedResultFromBackingStore,
                                          this,
                                          key,
                                          dependencyClock,
                                          _1);
                            
                            SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "reading EC for dep key %d...\n", key);
                            
                            m_backingStorage->Get(key, backingStoreReadComplete);
                            
                            return;
                        }
                        
                        SetResult(false);
                    }
                    
                    void CheckWriteRecursionCompletionHandler(TSelfPtr pParent,
                                                              TCurrentKeyProcessStatePtr pParentState,
                                                              const TStorageKey& key,
                                                              const TValueWrapperPtr bufferedWrite,
                                                              const TCausalClock& dependencyClock,
                                                              bool addToVersionApplierCheck,
                                                              bool result)
                    {
                        if(result)
                        {
                            //finishing the parent key will schedule its next tick
                            pParentState->Finish();
                        }
                        else
                        {
                            SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "RECURSE KEY %d FAILED\n", key);
                            
                            if(addToVersionApplierCheck)
                            {
                                m_versionApplier.AddToCheck(bufferedWrite);
                            }
                            
                            //still processing same key, so 'manually' restart the parent
                            m_versionApplier.ScheduleTask(pParent->m_runUpdate);
                        }
                    }
                    
                    void HandleReceivedResultFromBackingStore(const TStorageKey& key,
                                                              const TCausalClock& dependencyClock,
                                                              const TBoltOnSerializedValueTypePtr pResult)
                    {
                        SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "handle EC read result for key %d\n", key);
                        
                        if(pResult == nullptr)
                        {
                            SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "read EC for dep key %d, no result... \n", key);
                            SetResult(false);
                        }
                        else
                        {
                            TValueWrapperPtr result(m_serializer.Deserialize(pResult));
                            result->MarkFirstSeenByShim(); //todo -- how to propogate mutable changes here?
                            auto causality = result->Clock().Compare(dependencyClock);
                            bool shouldAdd = false;
                            
                            if(causality == CausalRelationship::Equal)
                            {
                                AddToConsidered(result, key, dependencyClock);
                                
                                shouldAdd = true;
                            }
                            else if(causality == CausalRelationship::HappensAfter || causality == CausalRelationship::Concurrent)
                            {
                                AddToConsidered(result, key, dependencyClock);
                                
                                m_pCurrentKeyProcessingState->HaltedOnTestBufferedWritesCovered();
                                
                                m_versionApplier.AddToCheck(result);
                                
                                boost::function<void(bool)> recursionComplete = boost::bind(&TSelf::CheckWriteRecursionCompletionHandler,
                                                                                            this,
                                                                                            this,
                                                                                            m_pCurrentKeyProcessingState,
                                                                                            key,
                                                                                            result,
                                                                                            dependencyClock,
                                                                                            true,
                                                                                            _1);
                                
                                SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "dep key %d is in EC but with a future write -- attempt recursive cover!\n", key);
                                
                                SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "clock for EC read for key %d:\n", key);
                                DEBUG_PrintClock(result->Clock().Data());
                                SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "...while dependencyClock for same key is:\n");
                                DEBUG_PrintClock(dependencyClock.Data());
                                
                                TSelf::TryCoverCut(m_versionApplier,
                                                   m_localShimStorage,
                                                   m_backingStorage->Clone(),
                                                   m_localStoreResolver,
                                                   m_serializer,
                                                   result,
                                                   m_keysToApply,
                                                   m_pKeyClockConsiderMap,
                                                   m_remainingDepth - 1,
                                                   recursionComplete
#if defined(SR_DEBUG_CutCoverageAttemptState_TrackRecurse)
                                                   ,m_recursionStack
#endif
                                                   );
                                
                                return;
                            }
                            
                            if(shouldAdd)
                            {
                                SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "found dep key %d in EC, can safely add it... \n", key);
                                m_pCurrentKeyProcessingState->Finish();
                                return;
                            }
                            else
                            {
                                SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "write for dep key %d not in EC, bail... \n", key);
                                m_versionApplier.AddToCheck(result);
                                SetResult(false);
                            }
                        }
                    }
                    
                    void AddToConsidered(const TValueWrapperPtr write,
                                         const TStorageKey& key,
                                         const TCausalClock& dependencyClock)
                    {
                        m_keysToApply.push_back(write);
                        AddToConsideredClocks(key, write->GetCausalClockForKey(key));
                    }
                    
                    void AddToConsideredClocks(const TStorageKey& key,
                                               const TCausalClock& dependencyClock)
                    {
                        SR_DEBUG_CutCoverageAttemptState_LOG(m_logging, "Adding considered write for key %d... \n", key);
                        DEBUG_PrintClock(dependencyClock.Data());
                        
                        m_versionApplier.AddToClockConsidered(key, dependencyClock, m_pKeyClockConsiderMap);
                    }
                    
                private:
                    boost::function<void()> m_runUpdate;
                    boost::function<void()> m_scheduleNextUpdate;
                    TVersionApplier& m_versionApplier;
                    TLocalShimStorage& m_localShimStorage;
                    std::unique_ptr<TBackingStorage> m_backingStorage;
                    TLocalStoreResolver& m_localStoreResolver;
                    TSerializer& m_serializer;
                    TValueWrapperPtr m_primaryValueWrapper;
                    
                    std::vector<TValueWrapperPtr>& m_keysToApply;
                    TKeyClockMapPtr m_pKeyClockConsiderMap;
                    size_t m_remainingDepth;
                    boost::function<void(bool)> m_clientCompletionCallback;
                    
                    
                    bool m_hasCheckedDependencyResultApplied;
                    std::vector<TStorageKey> m_dependencyKeys;
                    size_t m_dependencyKeyListCurrent;
                    
                    TCurrentKeyProcessStatePtr m_pCurrentKeyProcessingState;
                    bool m_hasCompleted;
                    bool m_result;
                    
                    bool m_logging;
                };
            }
        }
    }
}

#if defined SR_DEBUG_CutCoverageAttemptState_LOGGING
#undef SR_DEBUG_CutCoverageAttemptState_LOGGING
#endif

#undef SR_DEBUG_CutCoverageAttemptState_LOG

#if defined SR_DEBUG_CutCoverageAttemptState_TrackRecurse
#undef SR_DEBUG_CutCoverageAttemptState_TrackRecurse
#endif

