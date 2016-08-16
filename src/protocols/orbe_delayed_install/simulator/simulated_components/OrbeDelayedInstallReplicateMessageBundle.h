//
//  OrbeDelayedInstallReplicateMessageBundle.h
//  SimRunner
//
//  Created by Scott on 01/11/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <memory>
#include <boost/noncopyable.hpp>
#include "OrbeDelayedInstall.h"
#include "OrbeDelayedInstallReplicateMessageBundleRequestCompleteHandlerProxy.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            namespace SimulatedComponents
            {
                template<typename TProtocolTraits>
                class OrbeDelayedInstallReplicateMessageBundle : private boost::noncopyable
                {
                    typedef typename TProtocolTraits::TStorageKey TKey;
                    typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                    typedef typename TProtocolTraits::TNetworkLink TNetworkLink;
                    typedef typename TProtocolTraits::TClient TClient;
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
                    typedef OrbeDelayedInstallReplicateMessageBundle<TProtocolTraits> TSelf;
                    
                    TPartitionServer& m_partitionServer;
                    TReplicationMessageType m_replicateMessage;
                    TContext& m_context;
                    TNetworkLink& m_senderLink;
                    
                    OrbeDelayedInstallReplicateMessageBundle(TPartitionServer& partitionServer,
                                               TReplicationMessageType replicateMessage,
                                               TContext& context,
                                               TNetworkLink& senderLink)
                    : m_partitionServer(partitionServer)
                    , m_replicateMessage(replicateMessage)
                    , m_context(context)
                    , m_senderLink(senderLink)
                    {
                        m_senderLink.SendExternalNetworkMessage(OrbeDelayedInstallNetworkCompleteHandlerProxy<TContext>(m_context),
                                                                OrbeDelayedInstallReplicateMessageBundleRequestCompleteHandlerProxy<TSelf>(*this));
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
