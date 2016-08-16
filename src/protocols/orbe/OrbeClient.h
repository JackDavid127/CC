//
//  OrbeClient.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include "Timing.h"
#include "Orbe.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            template <typename TProtocolTraits>
            class OrbeClient : private boost::noncopyable
            {
                typedef OrbeClient<TProtocolTraits> TSelf;
                
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
                typedef typename TProtocolTraits::TDependencyMatrix TDependencyMatrix;
                
            public:
                OrbeClient(const ClientIdType& clientId,
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
                , m_dependencyMatrix()
                {
                }
                
                ~OrbeClient()
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
                    m_currentQueryPartitionId = m_keyToPartitionMapper.PartitionForKey(key);
                    const bool keyResidesOnLocalPartition = m_localPartitionServer.PartitionId() == m_currentQueryPartitionId;
                    
                    if(keyResidesOnLocalPartition)
                    {
                        m_localPartitionServer.Get(key, *this);
                    }
                    else
                    {
                        m_networkExchange.PerformGetAtLocalPartition(*this,
                                                                     key,
                                                                     m_localPartitionServer.ReplicaId(),
                                                                     m_currentQueryPartitionId,
                                                                     m_dependencyMatrix);
                    }
                }
                
                void IssuePut(const TKey& key, const TClientInputValueType& value)
                {
                    SR_ASSERT(!IsBusy());
                    m_busy = true;
                    m_currentQueryPartitionId = m_keyToPartitionMapper.PartitionForKey(key);
                    const bool keyResidesOnLocalPartition = m_localPartitionServer.PartitionId() == m_currentQueryPartitionId;
                    
                    if(keyResidesOnLocalPartition)
                    {
                        m_localPartitionServer.Put(key, value, *this, m_dependencyMatrix);
                    }
                    else
                    {
                        m_networkExchange.PerformPutAtLocalPartition(*this,
                                                                     key,
                                                                     value,
                                                                     m_localPartitionServer.ReplicaId(),
                                                                     m_currentQueryPartitionId,
                                                                     m_dependencyMatrix);
                    }
                }
                
                void HandleGetCompleteItemFound(const TClientGetReplyValueType& getReply)
                {
                    SR_ASSERT(IsBusy());
                    m_busy = false;
                    
                    m_dependencyMatrix.Update(m_currentQueryPartitionId,
                                              getReply.SourceReplicaId(),
                                              getReply.LogicalTimestamp());
                    
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
                    m_busy = false;
                    
                    //self.__metrics.client_add_put_to_key_partition(self.__client_name, partition_server_id)
                    
                    //self.__metrics.client_reset_dependency_matrix(self.__client_name)
                    
                    m_dependencyMatrix.Reset();
                    
                    m_dependencyMatrix.Update(m_currentQueryPartitionId,
                                              putReply.SourceReplicaId(),
                                              putReply.LogicalTimestamp());
                    
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
                TDependencyMatrix m_dependencyMatrix;
                PartitionIdType m_currentQueryPartitionId;
            };
        }
    }
}
