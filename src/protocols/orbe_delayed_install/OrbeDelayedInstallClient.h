//
//  OrbeDelayedInstallClient.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include "Timing.h"
#include "OrbeDelayedInstall.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            template <typename TProtocolTraits>
            class OrbeDelayedInstallClient : private boost::noncopyable
            {
                typedef OrbeDelayedInstallClient<TProtocolTraits> TSelf;
                
                typedef typename TProtocolTraits::TStorageKey TKey;
                typedef typename TProtocolTraits::TClientInputValueType TClientInputValueType;
                typedef typename TProtocolTraits::TClientGetReplyValueType TClientGetReplyValueType;
                typedef typename TProtocolTraits::TClientPutReplyValueType TClientPutReplyValueType;
                typedef typename TProtocolTraits::TMetrics TMetrics;
                typedef typename TProtocolTraits::TLogger TLogger;
                typedef typename TProtocolTraits::TKeyToPartitionMapper TKeyToPartitionMapper;
                typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                typedef typename TProtocolTraits::TNetworkExchange TNetworkExchange;
                typedef typename TProtocolTraits::TClientQueryResponseHandlerPtr TClientQueryResponseHandlerPtr;
                typedef typename TProtocolTraits::TClientDependencyTimestamp TClientDependencyTimestamp;
                typedef typename TProtocolTraits::TClientDependencyPartition TClientDependencyPartition;
                
            public:
                OrbeDelayedInstallClient(const ClientIdType& clientId,
                                         TKeyToPartitionMapper& keyToPartitionMapper,
                                         TNetworkExchange& networkExchange,
                                         TPartitionServer& localPartitionServer,
                                         const TClientQueryResponseHandlerPtr& pQueryResponseHandler,
                                         TMetrics& metrics,
                                         TLogger& logger)
                : m_clientId(clientId)
                , m_keyToPartitionMapper(keyToPartitionMapper)
                , m_networkExchange(networkExchange)
                , m_localPartitionServer(localPartitionServer)
                , m_pQueryResponseHandler(pQueryResponseHandler)
                , m_metrics(metrics)
                , m_logger(logger)
                , m_busy(false)
                , m_clientDependencyTimestamp(-1)
                , m_clientDependencyPartition(-1)
                , m_currentClientWritePartition(-1)
                {
                }
                
                ~OrbeDelayedInstallClient()
                {
                    
                }
                
                const ClientIdType& ClientId() const
                {
                    return m_clientId;
                }
                
                bool IsBusy() const
                {
                    return m_busy;
                }
                
                void IssueGet(const TKey& key)
                {
                    SR_ASSERT(!IsBusy());
                    m_busy = true;
                    auto currentQueryPartitionId = m_keyToPartitionMapper.PartitionForKey(key);
                    const bool keyResidesOnLocalPartition = m_localPartitionServer.PartitionId() == currentQueryPartitionId;
                    
                    if(keyResidesOnLocalPartition)
                    {
                        m_localPartitionServer.Get(key, m_clientId, *this);
                    }
                    else
                    {
                        m_networkExchange.PerformGetAtLocalPartition(*this,
                                                                     key,
                                                                     m_localPartitionServer.ReplicaId(),
                                                                     currentQueryPartitionId);
                    }
                }
                
                void IssuePut(const TKey& key, const TClientInputValueType& value)
                {
                    SR_ASSERT(!IsBusy());
                    SR_ASSERT(m_currentClientWritePartition == -1);
                    
                    m_busy = true;
                    m_currentClientWritePartition = m_keyToPartitionMapper.PartitionForKey(key);
                    const bool keyResidesOnLocalPartition = m_localPartitionServer.PartitionId() == m_currentClientWritePartition;
                   
                    m_logger.Log("Putting from client %d on replica %d to partition %d, with CDP: %d, CDT: %d\n",
                                 m_clientId,
                                 m_localPartitionServer.ReplicaId(),
                                 m_currentClientWritePartition,
                                 m_clientDependencyPartition,
                                 m_clientDependencyTimestamp);
                    
                    if(keyResidesOnLocalPartition)
                    {
                        m_localPartitionServer.Put(key,
                                                   value,
                                                   m_clientId,
                                                   m_clientDependencyTimestamp,
                                                   m_clientDependencyPartition,
                                                   *this);
                    }
                    else
                    {
                        m_networkExchange.PerformPutAtLocalPartition(*this,
                                                                     key,
                                                                     value,
                                                                     m_clientDependencyTimestamp,
                                                                     m_clientDependencyPartition,
                                                                     m_localPartitionServer.ReplicaId(),
                                                                     m_currentClientWritePartition);
                    }
                }
                
                void HandleGetCompleteItemFound(const TClientGetReplyValueType& getReply)
                {
                    SR_ASSERT(IsBusy());
                    m_busy = false;
                    
                    m_pQueryResponseHandler->HandleGetCompleteItemFound(getReply);
                }
                
                void HandleGetCompleteItemNotFound()
                {
                    SR_ASSERT(IsBusy());
                    m_busy = false;
                    
                    m_pQueryResponseHandler->HandleGetCompleteItemNotFound();
                }
                
                void HandlePutComplete(const TClientPutReplyValueType& putReply)
                {
                    SR_ASSERT(IsBusy());
                    SR_ASSERT(m_currentClientWritePartition >= 0);
                    
                    m_clientDependencyTimestamp = putReply.LogicalTimestamp();
                    m_clientDependencyPartition = m_currentClientWritePartition;
                    m_currentClientWritePartition = -1;
                    
                    m_busy = false;
                    
                    m_pQueryResponseHandler->HandlePutComplete(putReply);
                }
                
            private:
                ClientIdType m_clientId;
                TKeyToPartitionMapper& m_keyToPartitionMapper;
                TNetworkExchange& m_networkExchange;
                TPartitionServer& m_localPartitionServer;
                TClientQueryResponseHandlerPtr m_pQueryResponseHandler;
                TMetrics& m_metrics;
                TLogger& m_logger;
                bool m_busy;
                TClientDependencyTimestamp m_clientDependencyTimestamp;
                TClientDependencyPartition m_clientDependencyPartition;
                TClientDependencyPartition m_currentClientWritePartition;
            };
        }
    }
}
