//
//  OrbeDelayedInstallHeartbeatMessageBundle.h
//  SimRunner
//
//  Created by Scott on 06/01/2015.
//  Copyright (c) 2015 Scott. All rights reserved.
//

#pragma once

#include <memory>
#include <boost/noncopyable.hpp>
#include "OrbeDelayedInstall.h"
#include "OrbeDelayedInstallHeartbeatMessageBundleRequestCompleteHandlerProxy.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            namespace SimulatedComponents
            {
                template<typename TProtocolTraits>
                class OrbeDelayedInstallHeartbeatMessageBundle : private boost::noncopyable
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
                    typedef typename TProtocolTraits::TVersionVector TVersionVector;
                    typedef typename TProtocolTraits::TStorageComponent TStorage;
                    typedef typename TProtocolTraits::TMetrics TMetrics;
                    typedef typename TProtocolTraits::TDelayTracking TDelayTracking;
                    typedef typename TProtocolTraits::TLogger TLogger;
                    
                    typedef TNetworkExchange TContext;
                    typedef OrbeDelayedInstallHeartbeatMessageBundle<TProtocolTraits> TSelf;
                    
                    TPartitionServer& m_partitionServer;
                    ReplicaIdType m_senderReplicaId;
                    TVersionVector m_versionVector;
                    TContext& m_context;
                    TNetworkLink& m_senderLink;
                    
                    OrbeDelayedInstallHeartbeatMessageBundle(TPartitionServer& partitionServer,
                                                             ReplicaIdType& senderReplicaId,
                                                             TVersionVector& versionVector,
                                                             TContext& context,
                                                             TNetworkLink& senderLink)
                    : m_partitionServer(partitionServer)
                    , m_senderReplicaId(senderReplicaId)
                    , m_versionVector(versionVector)
                    , m_context(context)
                    , m_senderLink(senderLink)
                    {
                        m_senderLink.SendExternalNetworkMessage(OrbeDelayedInstallNetworkCompleteHandlerProxy<TContext>(m_context),
                                                                OrbeDelayedInstallHeartbeatMessageBundleRequestCompleteHandlerProxy<TSelf>(*this));
                    }
                    
                public:
                    static void Dispatch(TPartitionServer& partitionServer,
                                         ReplicaIdType senderReplicaId,
                                         TVersionVector versionVector,
                                         TContext& context,
                                         TNetworkLink& senderLink)
                    {
                        new TSelf(partitionServer, senderReplicaId, versionVector, context, senderLink);
                    }
                    
                    void Finish()
                    {
                        m_partitionServer.ReceiveHeartbeatVector(m_senderReplicaId, m_versionVector);
                        delete this;
                    }
                };
            }
        }
    }
}

