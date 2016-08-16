//
//  CurrentKeyProcessState.h
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
                enum KeyProcessingStage
                {
                    FirstRecursion = 0,
                    IO = 1,
                    SecondRecursion = 2,
                    NotBlocked = 3
                };
                
                template <typename TProtocolTraits>
                class CurrentKeyProcessState
                {
                    typedef typename TProtocolTraits::TStorageKey TStorageKey;
                    typedef typename TProtocolTraits::TCausalClock TCausalClock;
                    
                    
                public:
                    CurrentKeyProcessState(const TStorageKey& key,
                                           boost::function<void()>& finishedHandler)
                    : Key(key)
                    , m_finishedHandler(finishedHandler)
                    , PerformedComparisonToSelf(false)
                    , PerformedCheckForDependencyAlreadySatisfied(false)
                    , PerformedCheckAgainstBufferedWrites(false)
                    , PerformedReadBackingStore(false)
                    , m_exitState(KeyProcessingStage::NotBlocked)
                    , m_finished(false)
                    {

                    }
                    
                    bool HaltedOnTestBufferedWritesCovered() const
                    {
                        SR_ASSERT(!IsFinished());
                        return m_exitState == KeyProcessingStage::FirstRecursion;
                    }
                    
                    bool HaltedOnBackingStoreRead() const
                    {
                        SR_ASSERT(!IsFinished());
                        return m_exitState == KeyProcessingStage::IO;
                    }
                    
                    bool HaltedOnTestBackingStoreReadCovered() const
                    {
                        SR_ASSERT(!IsFinished());
                        return m_exitState == KeyProcessingStage::SecondRecursion;
                    }
                    
                    bool HaltedOnBlockingOperation() const
                    {
                        SR_ASSERT(!IsFinished());
                        return m_exitState != KeyProcessingStage::NotBlocked;
                    }
                    
                    bool IsFinished() const
                    {
                        return m_finished;
                    }
                    
                    void Finish()
                    {
                        SR_ASSERT(!IsFinished());
                        m_finished = true;
                        m_finishedHandler();
                    }
                    
                    void HaltOnTestBufferedWritesCovered()
                    {
                        SR_ASSERT(!IsFinished());
                        return m_exitState = KeyProcessingStage::FirstRecursion;
                    }
                    
                    void HaltOnBackingStoreRead()
                    {
                        SR_ASSERT(!IsFinished());
                        return m_exitState = KeyProcessingStage::IO;
                    }
                    
                    void HaltOnTestBackingStoreReadCovered()
                    {
                        SR_ASSERT(!IsFinished());
                        return m_exitState = KeyProcessingStage::SecondRecursion;
                    }
                    
                    void Unblock()
                    {
                        SR_ASSERT(!IsFinished());
                        SR_ASSERT(HaltedOnBlockingOperation());
                        m_exitState = KeyProcessingStage::NotBlocked;
                    }
                    
                private:
                    boost::function<void()> m_finishedHandler;
                    KeyProcessingStage m_exitState;
                    bool m_finished;
                    
                public: //todo -- fix --- ugly publics!
                    
                    TStorageKey Key;
                    bool PerformedComparisonToSelf;
                    bool PerformedCheckForDependencyAlreadySatisfied;
                    bool PerformedCheckAgainstBufferedWrites;
                    bool PerformedReadBackingStore;
                    
                    TCausalClock* DependencyClock;
                };
            }
        }
    }
}
