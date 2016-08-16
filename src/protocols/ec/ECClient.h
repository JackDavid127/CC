//
//  ECClient.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include "Timing.h"
#include "EC.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace EC
        {
            template <
            typename TKey,
            typename TClientInputValueType,
            typename TClientGetReplyValueType,
            typename TClientPutReplyValueType,
            typename TKeyToPartitionMapper,
            typename TMetrics,
            typename TLogger,
            typename TProtocolTraits
            >
            class ECClient : private boost::noncopyable
            {
                typedef ECClient<
                TKey,
                TClientInputValueType,
                TClientGetReplyValueType,
                TClientPutReplyValueType,
                TKeyToPartitionMapper,
                TMetrics,
                TLogger,
                TProtocolTraits
                > TSelf;
                
                typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                typedef typename TProtocolTraits::TNetworkExchange TNetworkExchange;
                typedef typename TProtocolTraits::TClientQueryResponseHandler TClientQueryResponseHandler;
                
            public:
                ECClient(const ClientIdType& clientId,
                         TKeyToPartitionMapper& keyToPartitionMapper,
                         TNetworkExchange& networkExchange,
                         TPartitionServer& localPartitionServer,
                         TClientQueryResponseHandler& queryResponseHandler,
                         TMetrics& metrics,
                         TLogger& logger)
                : m_clientId(clientId)
                , m_keyToPartitionMapper(keyToPartitionMapper)
                , m_networkExchange(networkExchange)
                , m_localPartitionServer(localPartitionServer)
                , m_queryResponseHandler(queryResponseHandler)
                , m_metrics(metrics)
                , m_logger(logger)
                , m_busy(false)
                {
                }
                
                ~ECClient()
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
                    m_busy = true;
                    const PartitionIdType partitionServerId = m_keyToPartitionMapper.PartitionForKey(key);
                    const bool keyResidesOnLocalPartition = m_localPartitionServer.PartitionId() == partitionServerId;
                    
                    if(keyResidesOnLocalPartition)
                    {
                        m_localPartitionServer.Get(key, *this);
                    }
                    else
                    {
                        m_networkExchange.PerformGetAtLocalPartition(ClientId(),
                                                                     *this,
                                                                     key,
                                                                     m_localPartitionServer.ReplicaId(),
                                                                     partitionServerId);
                    }
                }
                
                void IssuePut(const TKey& key, const TClientInputValueType& value)
                {
                    m_busy = true;
                    const PartitionIdType partitionServerId = m_keyToPartitionMapper.PartitionForKey(key);
                    const bool keyResidesOnLocalPartition = m_localPartitionServer.PartitionId() == partitionServerId;
                    
                    if(keyResidesOnLocalPartition)
                    {
                        m_localPartitionServer.Put(key, value, *this);
                    }
                    else
                    {
                        m_networkExchange.PerformPutAtLocalPartition(ClientId(),
                                                                     *this,
                                                                     key,
                                                                     value,
                                                                     m_localPartitionServer.ReplicaId(),
                                                                     partitionServerId);
                    }
                }
                
                void HandleGetCompleteItemFound(const TClientGetReplyValueType& value)
                {
                    m_busy = false;
                    m_queryResponseHandler.HandleGetCompleteItemFound(value);
                }
                
                void HandleGetCompleteItemNotFound()
                {
                    m_busy = false;
                    m_queryResponseHandler.HandleGetCompleteItemNotFound();
                }
                
                void HandlePutComplete(const TClientPutReplyValueType& value)
                {
                    m_busy = false;
                    m_queryResponseHandler.HandlePutComplete(value);
                }
                
                
            private:
                ClientIdType m_clientId;
                TKeyToPartitionMapper& m_keyToPartitionMapper;
                TNetworkExchange& m_networkExchange;
                TPartitionServer& m_localPartitionServer;
                TClientQueryResponseHandler& m_queryResponseHandler;
                TMetrics& m_metrics;
                TLogger& m_logger;
                bool m_busy;
            };
        }
    }
}
