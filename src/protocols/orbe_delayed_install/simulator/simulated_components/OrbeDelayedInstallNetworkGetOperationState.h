//
//  OrbeDelayedInstallNetworkGetOperationState.h
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
                class OrbeDelayedInstallNetworkGetOperationState : private boost::noncopyable
                {
                    typedef typename TProtocolTraits::TStorageKey TKey;
                    typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                    typedef typename TProtocolTraits::TNetworkLink TNetworkLink;
                    typedef typename TProtocolTraits::TClient TClient;
                    typedef typename TProtocolTraits::TClientId TClientId;
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
                    typedef OrbeDelayedInstallNetworkGetOperationState<TProtocolTraits, TCompletionHandler> TSelf;
                    
                public:
                    static void DispatchGet(const TKey& key,
                                            TPartitionServer& partitionServer,
                                            TNetworkLink& link,
                                            TContext& context,
                                            TCompletionHandler& getCompletedHandler,
                                            TLogger& logger,
                                            const TClientId& clientId)
                    {
                        (new TSelf(key,
                                   partitionServer,
                                   link,
                                   context,
                                   getCompletedHandler,
                                   logger,
                                   clientId))->Next();
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
                                m_partitionServer.Get(m_key, m_clientId, *this);
                            }break;
                                
                            case 2:
                            {
                                // finally, send the get response back to the requesting partition
                                m_link.SendInternalNetworkMessage(OrbeDelayedInstallNetworkCompleteHandlerProxy<TContext>(m_context),
                                                                  OrbeDelayedInstallNetworkOperationStateRequestCompleteHandlerProxy<TSelf>(*this));
                            }break;
                                
                            case 3:
                            {
                                if(m_gotGetValue)
                                {
                                    m_completionHandler.HandleGetCompleteItemFound(m_valueFromGet);
                                }
                                else
                                {
                                    m_completionHandler.HandleGetCompleteItemNotFound();
                                }
                                
                                delete this;
                            }break;
                                
                            default:
                            {
                                SR_ASSERT(false);
                            }break;
                        }
                    }
                    
                    void HandleGetCompleteItemFound(const TClientGetReplyValueType& value)
                    {
                        m_valueFromGet = value;
                        m_gotGetValue = true;
                        Next();
                    }
                    
                    void HandleGetCompleteItemNotFound()
                    {
                        m_gotGetValue = false;
                        Next();
                    }
                    
                private:
                    OrbeDelayedInstallNetworkGetOperationState(const TKey& key,
                                                               TPartitionServer& partitionServer,
                                                               TNetworkLink& link,
                                                               TContext& context,
                                                               TCompletionHandler& completionHandler,
                                                               TLogger& logger,
                                                               const TClientId& clientId)
                    : m_key(key)
                    , m_partitionServer(partitionServer)
                    , m_link(link)
                    , m_context(context)
                    , m_completionHandler(completionHandler)
                    , m_logger(logger)
                    , m_phase(0)
                    , m_gotGetValue(false)
                    , m_clientId(clientId)
                    {
                        
                    }
                    
                    TKey m_key;
                    TPartitionServer& m_partitionServer;
                    TNetworkLink& m_link;
                    TContext& m_context;
                    TCompletionHandler& m_completionHandler;
                    TLogger& m_logger;
                    TClientId m_clientId;
                    size_t m_phase;
                    TClientGetReplyValueType m_valueFromGet;
                    bool m_gotGetValue;
                };
            }
        }
    }
}

