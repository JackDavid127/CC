//
//  OrbeReplicateMessageBundle.h
//  SimRunner
//
//  Created by Scott on 01/11/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <memory>
#include <boost/noncopyable.hpp>
#include "Orbe.h"
#include "OrbeReplicateMessageBundleRequestCompleteHandlerProxy.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            namespace SimulatedComponents
            {
                template<typename TProtocolTraits>
                class OrbeReplicateMessageBundle : private boost::noncopyable
                {
                    typedef typename TProtocolTraits::TStorageKey TKey;
                    typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                    typedef typename TProtocolTraits::TNetworkLink TNetworkLink;
                    typedef typename TProtocolTraits::TClient TClient;
                    typedef typename TProtocolTraits::TDependencyMatrix TDependencyMatrix;
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
                    typedef OrbeReplicateMessageBundle<TProtocolTraits> TSelf;
                    
                    TPartitionServer& m_partitionServer;
                    TReplicationMessageType m_replicateMessage;
                    TContext& m_context;
                    TNetworkLink& m_senderLink;
                    
                    OrbeReplicateMessageBundle(TPartitionServer& partitionServer,
                                               TReplicationMessageType replicateMessage,
                                               TContext& context,
                                               TNetworkLink& senderLink)
                    : m_partitionServer(partitionServer)
                    , m_replicateMessage(replicateMessage)
                    , m_context(context)
                    , m_senderLink(senderLink)
                    {
                        m_senderLink.SendExternalNetworkMessage(OrbeNetworkCompleteHandlerProxy<TContext>(m_context),
                                                                OrbeReplicateMessageBundleRequestCompleteHandlerProxy<TSelf>(*this));
                    }
                    
                public:
                    static void Dispatch(TPartitionServer& partitionServer,
                                         TReplicationMessageType replicateMessage,
                                         TContext& context,
                                         TNetworkLink& senderLink)
                    {
                        new TSelf(partitionServer, replicateMessage, context, senderLink);
                    }
                    
                    void Finish()
                    {
                        m_partitionServer.ReceiveReplicateMessage(m_replicateMessage);
                        delete this;
                    }
                };
            }
        }
    }
}
