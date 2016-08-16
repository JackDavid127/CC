//
//  AsynchronousResolver.h
//  SimRunner
//
//  Created by Scott on 15/12/2014.
//

#pragma once

#include <boost/noncopyable.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <set>
#include <vector>
#include "Protocols.h"
//#include "BoltOnMessaging.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            template <typename TProtocolTraits>
            class AsynchronousResolver : private boost::noncopyable
            {
                typedef typename TProtocolTraits::TBackingStorage TBackingStorage;
                typedef typename TProtocolTraits::TLocalShimStorage TLocalShimStorage;
                typedef typename TProtocolTraits::TTaskRunner TTaskRunner;
                typedef typename TProtocolTraits::TVersionApplier TVersionApplier;
                typedef typename TProtocolTraits::TValueWrapperPtr TValueWrapperPtr;
                typedef typename TProtocolTraits::TStorageKey TStorageKey;
                typedef typename TProtocolTraits::TValueWrapperFactory TValueWrapperFactory;
                typedef typename TProtocolTraits::TBoltOnSerializer TSerializer;
                typedef typename TProtocolTraits::TBoltOnSerializedValueTypePtr TBoltOnSerializedValueTypePtr;
                
            public:
                AsynchronousResolver(TBackingStorage& backingStorage,
                                     TLocalShimStorage& localShimStorage,
                                     TTaskRunner& taskRunner,
                                     TSerializer& serializer,
                                     TValueWrapperFactory& valueWrapperFactory)
                : m_backingStorage(backingStorage.Clone())
                , m_localShimStorage(localShimStorage)
                , m_serializer(serializer)
                , m_versionApplier(backingStorage, m_localShimStorage, *this, serializer, taskRunner, valueWrapperFactory)
                , m_maxKeysToCheck(0)
                , m_limitNumberOfKeysToCheck(false)
                {
                    
                }
                
                const TVersionApplier& VersionApplier() const
                {
                    return m_versionApplier;
                }
                
                void AddKeyToCheck(const TStorageKey& key)
                {
                    if(CanAddKeyToCheck())
                    {
                        auto findResult = m_keysToCheck.find(key);
                        
                        if(findResult != m_keysToCheck.end())
                        {
                            m_keysToCheck.erase(findResult);
                        }
                        
                        auto result = m_keysToCheck.insert(key);
                        SR_ASSERT(result.second);
                        
                    }
                }
                
                void CheckSingleKey(const TValueWrapperPtr valueWrapper,
                                    size_t currentBackingStoreReadCount,
                                    boost::function<void (const TValueWrapperPtr, bool)> clientCompletionHandler)
                {
                    boost::function<void (bool)> coverageCheckCompletionHandler = boost::bind(&AsynchronousResolver::HandleSingleKeyCheckComplete,
                                                                                              this,
                                                                                              clientCompletionHandler,
                                                                                              valueWrapper,
                                                                                              _1);
                    
                    m_versionApplier.CheckSingleKey(valueWrapper,
                                                    currentBackingStoreReadCount,
                                                    coverageCheckCompletionHandler);
                }
                
                void HandleSingleKeyCheckComplete(boost::function<void (const TValueWrapperPtr, bool)> clientCompletionHandler,
                                                  const TValueWrapperPtr valueWrapper,
                                                  bool covered)
                {
                    if(covered)
                    {
                        clientCompletionHandler(valueWrapper, true);
                    }
                    else
                    {
                        m_versionApplier.AddToCheck(valueWrapper);
                        clientCompletionHandler(valueWrapper, false);
                    }
                }
                
                void Update()
                {
                    std::vector<TStorageKey>* pKeysToCheck = new std::vector<TStorageKey>(m_keysToCheck.begin(), m_keysToCheck.end());
                    m_keysToCheck.clear();
                    HandleBufferedKeyRetrieved(pKeysToCheck, nullptr);
                    m_versionApplier.DrainEvents();
                }
                
            private:
                bool CanAddKeyToCheck()
                {
                    if(m_limitNumberOfKeysToCheck)
                    {
                        return m_keysToCheck.size() < m_maxKeysToCheck;
                    }
                    else
                    {
                        return true;
                    }
                }
                
                void HandleBufferedKeyRetrieved(std::vector<TStorageKey>* pKeysToCheck, const TBoltOnSerializedValueTypePtr pResult)
                {
                    if(pResult != nullptr)
                    {
                        m_versionApplier.AddToCheck(m_serializer.Deserialize(pResult));
                    }
                    
                    if(pKeysToCheck->size() == 0)
                    {
                        delete pKeysToCheck;
                    }
                    else
                    {
                        pKeysToCheck->pop_back();
                        
                        boost::function<void (const TBoltOnSerializedValueTypePtr)> binding = boost::bind(&AsynchronousResolver::HandleBufferedKeyRetrieved,
                                                                                                          this,
                                                                                                          pKeysToCheck,
                                                                                                          _1);
                        
                        m_backingStorage->Get(pKeysToCheck->back(), binding);
                        
                        m_versionApplier.TryApplyAllPossible();
                    }
                }
                
                std::unique_ptr<TBackingStorage> m_backingStorage;
                TLocalShimStorage& m_localShimStorage;
                TSerializer& m_serializer;
                TVersionApplier m_versionApplier;
                std::set<TStorageKey> m_keysToCheck;
                size_t m_maxKeysToCheck;
                bool m_limitNumberOfKeysToCheck;
            };
        }
    }
}
