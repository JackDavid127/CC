//
//  AsynchronousReadShimBackEnd.h
//  SimRunner
//
//  Created by Scott on 15/12/2014.
//

#pragma once

#include <boost/noncopyable.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <vector>
#include "BoltOn.h"
#include "Timing.h"
#include <stdio.h>

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            template<typename TProtocolTraits>
            class AsynchronousReadShimBackEnd : private boost::noncopyable
            {
                typedef typename TProtocolTraits::TShimId TShimId;
                typedef typename TProtocolTraits::TBackingStorage TBackingStorage;
                typedef typename TProtocolTraits::TLocalShimStorage TLocalShimStorage;
                typedef typename TProtocolTraits::TLocalStoreResolver TLocalStoreResolver;
                typedef typename TProtocolTraits::TStorageKey TStorageKey;
                typedef typename TProtocolTraits::TCounter TCounter;
                typedef typename TProtocolTraits::TValueWrapperPtr TValueWrapperPtr;
                typedef typename TProtocolTraits::TValueWrapperFactory TValueWrapperFactory;
                typedef typename TProtocolTraits::TCausalClock TCausalClock;
                typedef typename TProtocolTraits::TKeyDependencies TKeyDependencies;
                typedef typename TProtocolTraits::TClientInputValueType TClientInputValueType;
                typedef typename TProtocolTraits::TBoltOnSerializer TSerializer;
                typedef typename TProtocolTraits::TBoltOnSerializedValueTypePtr TBoltOnSerializedValueTypePtr;

            public:
                AsynchronousReadShimBackEnd(TShimId& shimId,
                                            TLocalShimStorage& localShimStorage,
                                            TLocalStoreResolver& localStoreResolver,
                                            TSerializer& serializer,
                                            TValueWrapperFactory& valueWrapperFactory)
                : m_shimId(shimId)
                , m_localShimStorage(localShimStorage)
                , m_localStoreResolver(localStoreResolver)
                , m_serializer(serializer)
                , m_valueWrapperFactory(valueWrapperFactory)
                , m_maxNumReadsECDS(0) //make (> 0) for pessimistic, (0) for optimistic
                , m_localSequenceNum(0)
                , m_realNullReadsCount(0)
                , m_falseNullReadsCount(0)
                , m_numGets(0)
                , m_numPuts(0)
                {

                }

                TCounter RealNullReads()
                {
                    return m_realNullReadsCount;
                }

                TCounter FalseNullReads()
                {
                    return m_falseNullReadsCount;
                }

                TCounter Gets()
                {
                    return m_numGets;
                }

                TCounter Puts()
                {
                    return m_numPuts;
                }

                void Get(TBackingStorage& backingStorage,
                         const TStorageKey& key,
                         const boost::function<void (const TValueWrapperPtr)>& getCompleteHandler)
                {
                    //self.__num_gets.increment_and_get()

                    if(m_maxNumReadsECDS == 0)
                    {
                        m_localStoreResolver.AddKeyToCheck(key);
                        TValueWrapperPtr* pValueWrapper;
                        //printf("Entering GET\n");

                        if(m_localShimStorage.TryGetValue(key, pValueWrapper))
                        {
                            getCompleteHandler(*pValueWrapper);
                        }
                        else
                        {
                            //self.__real_null_reads_count.increment_and_get()
                            getCompleteHandler(nullptr);
                        }
                    }
                    else
                    {
                        boost::function<void (const TBoltOnSerializedValueTypePtr)> binding = boost::bind(&AsynchronousReadShimBackEnd::HandleGetComplete,
                                                                                                          this,
                                                                                                          getCompleteHandler,
                                                                                                          key,
                                                                                                          _1);


                        backingStorage.Get(key, binding);
                    }
                }

                void Gets(TBackingStorage& backingStorage,
                         const std::vector<TStorageKey>& keys,
                         const boost::function<void (const TValueWrapperPtr)>& getCompleteHandler)
                {
                    Utilities::TTimestamp mst, med;
                    for (auto it=keys.begin();it!=keys.end();it++){
                        if(m_maxNumReadsECDS == 0)
                        {
                            m_localStoreResolver.AddKeyToCheck(*it);
                            TValueWrapperPtr* pValueWrapper;
                            Utilities::TTimestamp st, ed;
                            //printf("Entering GET\n");

                            if(m_localShimStorage.TryGetValue(*it, pValueWrapper, st, ed))
                            {
                                printf("trans: ");getCompleteHandler(*pValueWrapper);
                                if(it == keys.begin()) mst=st, med=ed;
                                else if(st>=med || ed<=mst){
                                    printf("Invalid Interval\n");
                                    if(m_localShimStorage.TryGetValue(*it, pValueWrapper, mst)){
                                        printf("1trans: ");getCompleteHandler(*pValueWrapper);
                                    }
                                    else if(m_localShimStorage.TryGetValue(*it, pValueWrapper, med)){
                                        printf("2trans: ");getCompleteHandler(*pValueWrapper);
                                    }
                                    else{
                                        printf("Read Transaction Failed");
                                        return;
                                    }
                                }
                                else{
                                    if(st>mst) mst=st;
                                    if(ed<med) med=ed;
                                }
                            }
                            else
                            {
                                //self.__real_null_reads_count.increment_and_get()
                                getCompleteHandler(nullptr);
                                printf("Read Transaction Failed\n");
                                return;
                            }
                        }
                        else
                        {
                            boost::function<void (const TBoltOnSerializedValueTypePtr)> binding = boost::bind(&AsynchronousReadShimBackEnd::HandleGetComplete,
                                                                                                              this,
                                                                                                              getCompleteHandler,
                                                                                                              *it,
                                                                                                              _1);


                            backingStorage.Get(*it, binding);
                        }
                    }
                    printf("Read Transaction Succeeded\n");
                }

                TValueWrapperPtr Put(TBackingStorage& backingStorage,
                                     const TStorageKey& key,
                                     const TClientInputValueType& value,
                                     const boost::function<void (const TValueWrapperPtr)>& putCompleteHandler)
                {
                    std::set<TValueWrapperPtr> dependencies;
                    return PutAfterDependencies(backingStorage, key, value, dependencies, putCompleteHandler);
                }

                TValueWrapperPtr PutAfterDependency(TBackingStorage& backingStorage,
                                                    const TStorageKey& key,
                                                    const TClientInputValueType& value,
                                                    const TValueWrapperPtr dependency,
                                                    const boost::function<void (const TValueWrapperPtr)>& putCompleteHandler)
                {
                    std::set<TValueWrapperPtr> dependencies;
                    auto result = dependencies.insert(dependency);
                    SR_ASSERT(result.second);
                    return PutAfterDependencies(backingStorage, key, value, dependencies, putCompleteHandler);
                }

                TValueWrapperPtr PutAfterDependencies(TBackingStorage& backingStorage,
                                                      const TStorageKey& key,
                                                      const TClientInputValueType& value,
                                                      const std::set<TValueWrapperPtr>& dependencies,
                                                      const boost::function<void (const TValueWrapperPtr)>& putCompleteHandler)
                {
                    //self.__num_puts.increment_and_get()

                    TKeyDependencies keyDependencies(dependencies);
                    TCausalClock aggregateClock = TCausalClock::CreateEmpty();

                    for(auto it = dependencies.begin(); it != dependencies.end(); ++ it)
                    {
                        const TValueWrapperPtr valueWrapper(*it);
                        if(key == valueWrapper->Key())
                        {
                            aggregateClock.MergeWithClock(valueWrapper->Clock());
                        }
                    }

                    aggregateClock.SetValueForShim(m_shimId, m_localSequenceNum.IncrementAndGet());

                    keyDependencies.PutDependency(key, aggregateClock);

                    TValueWrapperPtr result(m_valueWrapperFactory.CreateValueWrapper(key, value, keyDependencies, Utilities::Now()));

                    boost::function<void ()> binding = boost::bind(&AsynchronousReadShimBackEnd::HandlePutComplete,
                                                                   this,
                                                                   putCompleteHandler,
                                                                   result);

                    backingStorage.Put(key, result->ToData(), binding);
                    m_localShimStorage.AddValueForKey(key, result);

                    return result;
                }

            private:

                void HandleGetComplete(const boost::function<void (const TValueWrapperPtr)>& clientGetCompleteHandler,
                                       const TStorageKey& key,
                                       const TBoltOnSerializedValueTypePtr pSerializedResult)
                {
                    printf("%%Entering HandleGetComplete\n");
                    if(pSerializedResult != nullptr)
                    {
                        boost::function<void (const TValueWrapperPtr, bool)> binding = boost::bind(&AsynchronousReadShimBackEnd::HandleCoverageCheckCompleted,
                                                                                                   this,
                                                                                                   clientGetCompleteHandler,
                                                                                                   _1,
                                                                                                   _2);

                        TValueWrapperPtr protocolItem = m_serializer.Deserialize(pSerializedResult);
                        m_localStoreResolver.CheckSingleKey(protocolItem, m_maxNumReadsECDS - 1, binding);
                    }
                    else
                    {
                        TValueWrapperPtr* pValueWrapper;

                        if(m_localShimStorage.TryGetValue(key, pValueWrapper))
                        {
                            //self.__stale_reads_count.increment_and_get()
                            clientGetCompleteHandler(*pValueWrapper);
                        }
                        else
                        {
                            //self.__real_null_reads_count.increment_and_get()
                            clientGetCompleteHandler(nullptr);
                        }
                    }
                }

                void HandlePutComplete(const boost::function<void (const TValueWrapperPtr)>& clientPutCompleteHandler,
                                       const TValueWrapperPtr valueWrapper)
                {
                    clientPutCompleteHandler(valueWrapper);
                }

                void HandleCoverageCheckCompleted(const boost::function<void (const TValueWrapperPtr)>& clientGetCompleteHandler,
                                                  const TValueWrapperPtr coverageAttemptItem,
                                                  bool successfullyCoveredItem)
                {
                    if(successfullyCoveredItem)
                    {
                        clientGetCompleteHandler(coverageAttemptItem);
                    }
                    else
                    {
                        TValueWrapperPtr* pFallback;

                        if(m_localShimStorage.TryGetValue(coverageAttemptItem->Key(), pFallback))
                        {
                            //self.__stale_reads_count.increment_and_get()
                            clientGetCompleteHandler(*pFallback);
                        }
                        else
                        {
                            //self.__false_null_reads_count.increment_and_get()
                            clientGetCompleteHandler(nullptr);
                        }
                    }
                }

                TShimId& m_shimId;
                TLocalShimStorage& m_localShimStorage;
                TLocalStoreResolver& m_localStoreResolver;
                TSerializer& m_serializer;
                TValueWrapperFactory& m_valueWrapperFactory;
                size_t m_maxNumReadsECDS;
                TCounter m_localSequenceNum;
                TCounter m_realNullReadsCount;
                TCounter m_falseNullReadsCount;
                TCounter m_numGets;
                TCounter m_numPuts;
            };
        }
    }
}
