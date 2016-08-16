//
//  OrbeDelayedInstallNetworkPutOperationState.h
//  SimRunner
//
//  Created by Scott on 01/11/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <memory>
#include <boost/noncopyable.hpp>
#include "OrbeDelayedInstall.h"
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            namespace SimulatedComponents
            {
                template<typename TProtocolTraits, typename TCompletionHandler>
                class OrbeDelayedInstallNetworkPutOperationState : private boost::noncopyable
                {
                    typedef typename TProtocolTraits::TStorageKey TKey;
                    typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                    typedef typename TProtocolTraits::TNetworkLink TNetworkLink;
                    typedef typename TProtocolTraits::TClient TClient;
                    typedef typename TProtocolTraits::TClientId TClientId;
                    typedef typename TProtocolTraits::TClientDependencyTimestamp TClientDependencyTimestamp;
                    typedef typename TProtocolTraits::TClientDependencyPartition TClientDependencyPartition;
                    typedef typename TProtocolTraits::TNetworkExchange TNetworkExchange;
                    typedef typename TProtocolTraits::TBroadcaster TBroadcaster;
                    typedef typename TProtocolTraits::TStorageKey TStorageKey;
                    typedef typename TProtocolTraits::TSerializer TSerializer;
                    typedef typename TProtocolTraits::TSerializedDataType TSerializedDataType;
                    typedef typename TProtocolTraits::TDeserializedValueType TDeserializedValueType;
                    typedef typename TProtocolTraits::TClientInputValueType TClientInputValueType;
                    typedef typename TProtocolTraits::TClientGetReplyValueType TClientGetReplyValueType;
                    typedef typename TProtocolTraits::TClientPutReplyValueType TClientPutReplyValueType;
                    typedef typename TProtocolTraits::TReplicationMessageType TReplicationMessageType;
                    typedef typename TProtocolTraits::TStorageComponent TStorage;
                    typedef typename TProtocolTraits::TMetrics TMetrics;
                    typedef typename TProtocolTraits::TDelayTracking TDelayTracking;
                    typedef typename TProtocolTraits::TLogger TLogger;
                    
                    typedef TNetworkExchange TContext;
                    typedef OrbeDelayedInstallNetworkPutOperationState<TProtocolTraits, TCompletionHandler> TSelf;
                    
                public:
                    static void DispatchPut(const TKey& key,
                                            const TClientInputValueType& value,
                                            TPartitionServer& partitionServer,
                                            TNetworkLink& link,
                                            TContext& context,
                                            TCompletionHandler& putCompletedHandler,
                                            TLogger& logger,
                                            const TClientId& clientId,
                                            const TClientDependencyTimestamp& clientDependencyTimestamp,
                                            const TClientDependencyPartition& clientDependencyPartition)
                    {
                        (new OrbeDelayedInstallNetworkPutOperationState(key,
                                                                        value,
                                                                        partitionServer,
                                                                        link,
                                                                        context,
                                                                        putCompletedHandler,
                                                                        logger,
                                                                        clientId,
                                                                        clientDependencyTimestamp,
                                                                        clientDependencyPartition))->Next();
                    }
                    
                    void Next()
                    {
                        size_t currentPhase = m_phase++;
                        
                        switch(currentPhase)
                        {
                            case 0:
                            {
                                // send the message to the other partition server
                                m_link.SendInternalNetworkMessage(OrbeDelayedInstallNetworkCompleteHandlerProxy<TContext>(m_context),
                                                                  OrbeDelayedInstallNetworkOperationStateRequestCompleteHandlerProxy<TSelf>(*this));
                            }break;
                                
                            case 1:
                            {
                                // do the operation at the local partition server
                                m_partitionServer.Put(m_key,
                                                      m_putValue,
                                                      m_clientId,
                                                      m_clientDependencyTimestamp,
                                                      m_clientDependencyPartition,
                                                      *this);
                            }break;
                                
                            case 2:
                            {
                                // finally, send the get response back to the requesting partition
                                m_link.SendInternalNetworkMessage(OrbeDelayedInstallNetworkCompleteHandlerProxy<TContext>(m_context),
                                                                  OrbeDelayedInstallNetworkOperationStateRequestCompleteHandlerProxy<TSelf>(*this));
                            }break;
                                
                            case 3:
                            {
                                SR_ASSERT(m_gotPutValue);
                                m_completionHandler.HandlePutComplete(m_valueFromPut);
                                delete this;
                            }break;
                                
                            default:
                            {
                                SR_ASSERT(false);
                            }break;
                        }
                    }
                    
                    void HandlePutComplete(const TClientPutReplyValueType& value)
                    {
                        m_valueFromPut = value;
                        m_gotPutValue = true;
                        Next();
                    }
                    
                private:
                    OrbeDelayedInstallNetworkPutOperationState(const TKey& key,
                                                               const TClientInputValueType& valueToPut,
                                                               TPartitionServer& partitionServer,
                                                               TNetworkLink& link,
                                                               TContext& context,
                                                               TCompletionHandler& completionHandler,
                                                               TLogger& logger,
                                                               const TClientId& clientId,
                                                               const TClientDependencyTimestamp& clientDependencyTimestamp,
                                                               const TClientDependencyPartition& clientDependencyPartition)
                    : m_key(key)
                    , m_putValue(valueToPut)
                    , m_partitionServer(partitionServer)
                    , m_link(link)
                    , m_context(context)
                    , m_completionHandler(completionHandler)
                    , m_logger(logger)
                    , m_phase(0)
                    , m_gotPutValue(false)
                    , m_clientId(clientId)
                    , m_clientDependencyTimestamp(clientDependencyTimestamp)
                    , m_clientDependencyPartition(clientDependencyPartition)
                    {
                        
                    }
                    
                    TKey m_key;
                    TPartitionServer& m_partitionServer;
                    TNetworkLink& m_link;
                    TContext& m_context;
                    TCompletionHandler& m_completionHandler;
                    TLogger& m_logger;
                    TClientId m_clientId;
                    TClientDependencyTimestamp m_clientDependencyTimestamp;
                    TClientDependencyPartition m_clientDependencyPartition;
                    size_t m_phase;
                    TClientInputValueType m_putValue;
                    TClientPutReplyValueType m_valueFromPut;
                    bool m_gotPutValue;
                };
            }
        }
    }
}

