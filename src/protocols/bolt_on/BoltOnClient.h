//
//  BoltOnClient.h
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
#include <cstdio>
//#include "BoltOnMessaging.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            template <typename TProtocolTraits>
            class BoltOnClient : private boost::noncopyable
            {
                typedef typename TProtocolTraits::TMetrics TMetrics;
                typedef typename TProtocolTraits::TShimDeployment TShimDeployment;
                typedef typename TProtocolTraits::TStorageKey TStorageKey;
                typedef typename TProtocolTraits::TClientInputValueType TClientInputValueType;
                typedef typename TProtocolTraits::TClientQueryResponseHandler TClientQueryResponseHandler;
                typedef typename TProtocolTraits::TBackingStorage TBackingStorage;
                typedef typename TProtocolTraits::TValueWrapperPtr TValueWrapperPtr;

            public:
                BoltOnClient(const ClientIdType& clientId,
                             TShimDeployment& boltOnShim,
                             TClientQueryResponseHandler& queryResponseHandler,
                             TBackingStorage& backingStorage,
                             TMetrics& metrics,
                             bool fullCausality)
                : m_clientId(clientId)
                , m_boltOnShim(boltOnShim)
                , m_queryResponseHandler(queryResponseHandler)
                , m_backingStorage(backingStorage)
                , m_metrics(metrics)
                , m_fullCausality(fullCausality)
                , m_busy(false)
                , m_currentChainLen(0)
                , m_maxChainLen(5)
                {

                }

                bool IsBusy() const
                {
                    return m_busy;
                }

                void IssueGet(const TStorageKey& key)
                {
                    //printf("get -> %d\n", m_clientId);

                    boost::function<void (const TValueWrapperPtr)> binding = boost::bind(&BoltOnClient::HandleGetComplete,
                                                                                         this,
                                                                                         _1);

                    m_busy = true;
                    m_boltOnShim.Get(m_backingStorage, key, binding);
                }

                void IssueGets(const std::vector<TStorageKey>& keys)
                {
                    boost::function<void (const std::vector<TValueWrapperPtr>)> binding = boost::bind(&BoltOnClient::HandleGetsComplete,
                                                                                         this,
                                                                                         _1);

                    m_busy = true;
                    m_boltOnShim.Gets(m_backingStorage, keys, binding);
                }

                void IssuePut(const TStorageKey& key,
                              const TClientInputValueType& value)
                {
                    //printf("put -> %d\n", m_clientId);

                    boost::function<void (const TValueWrapperPtr)> binding = boost::bind(&BoltOnClient::HandleBoltOnPutComplete,
                                                                                         this,
                                                                                         _1);

                    TValueWrapperPtr write = m_boltOnShim.PutAfterDependencies(m_backingStorage, key, value, m_dependencies, binding);
                    printf("Use Count: ");
                    for (const TValueWrapperPtr &p: m_dependencies) printf("%d ",p.use_count());
                    printf("\n");
                    m_dependencies.clear();

                    if(m_fullCausality)
                    {
                        m_dependencies.insert(write);
                    }
                    else
                    {
                        if(m_currentChainLen < m_maxChainLen)
                        {
                            m_dependencies.insert(write);
                            m_currentChainLen += 1;
                        }
                        else
                        {
                            m_currentChainLen = 0;
                        }
                    }
                }

                //todo -- remove stubs needed for ec interop
                template <typename TUnknownMsg> void HandleGetCompleteItemFound(const TUnknownMsg& _) { SR_ASSERT(false); }
                void HandleGetCompleteItemNotFound() { SR_ASSERT(false); }
                template <typename TUnknownMsg> void HandlePutComplete(const TUnknownMsg& _) { SR_ASSERT(false); }

            private:

                void HandleGetComplete(const TValueWrapperPtr pWrite)
                {
                    //printf("get done -> %d", m_clientId);

                    if(pWrite != nullptr)
                    {
                        //printf("get done -> %d (%d, %d)\n", m_clientId, pWrite->Key(), pWrite->Value());
                        m_dependencies.insert(pWrite);
                        m_queryResponseHandler.HandleGetCompleteItemFound(pWrite);
                    }
                    else
                    {
                        //printf("failed\n");
                        m_queryResponseHandler.HandleGetCompleteItemNotFound();
                    }

                    m_busy = false;
                }
                
                void HandleGetsComplete(const std::vector<TValueWrapperPtr> pWrite)
                {
                    //printf("get done -> %d", m_clientId);

                    if(pWrite.size() > 0)
                    {
                        //printf("get done -> %d (%d, %d)\n", m_clientId, pWrite->Key(), pWrite->Value());
                        for (auto p: pWrite) m_dependencies.insert(p);
                        m_queryResponseHandler.HandleGetsCompleteItemFound(pWrite);
                    }
                    else
                    {
                        //printf("failed\n");
                        m_queryResponseHandler.HandleGetsCompleteItemNotFound();
                    }

                    m_busy = false;
                }

                void HandleBoltOnPutComplete(const TValueWrapperPtr pWrite)
                {
                    //printf("put done -> %d\n", m_clientId);
                    m_queryResponseHandler.HandlePutComplete(pWrite);
                    m_busy = false;
                }

                ClientIdType m_clientId;
                TShimDeployment& m_boltOnShim;
                TClientQueryResponseHandler& m_queryResponseHandler;
                TBackingStorage& m_backingStorage;
                TMetrics& m_metrics;
                bool m_fullCausality;
                bool m_busy;
                std::set<TValueWrapperPtr> m_dependencies;
                size_t m_currentChainLen;
                size_t m_maxChainLen;
            };
        }
    }
}
