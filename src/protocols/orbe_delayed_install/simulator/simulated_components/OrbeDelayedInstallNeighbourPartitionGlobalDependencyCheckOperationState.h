//
//  OrbeDelayedInstallNeighbourPartitionGlobalDependencyCheckOperationState.h
//  SimRunner
//
//  Created by Scott on 07/01/2015.
//  Copyright (c) 2015 Scott. All rights reserved.
//

#pragma once

#include <memory>
#include <boost/noncopyable.hpp>
#include "OrbeDelayedInstall.h"
#include "SimRunnerAssert.h"
#include "OrbeDelayedInstallPartitionServerReplicationHandler.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            namespace SimulatedComponents
            {
                template<typename TProtocolTraits, typename TCompletionHandler>
                class OrbeDelayedInstallNeighbourPartitionGlobalDependencyCheckOperationState : private boost::noncopyable
                {
                    typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                    typedef typename TProtocolTraits::TNetworkLink TNetworkLink;
                    typedef typename TProtocolTraits::TClientDependencyTimestamp TClientDependencyTimestamp;
                    typedef typename TProtocolTraits::TNetworkExchange TContext;
                    typedef typename TProtocolTraits::TLogger TLogger;
                    
                    typedef OrbeDelayedInstallNeighbourPartitionGlobalDependencyCheckOperationState<TProtocolTraits, TCompletionHandler> TSelf;
                    
                public:
                    static void DispatchRequest(TPartitionServer& neighbourPartitionServer,
                                                TNetworkLink& link,
                                                PartitionIdType requesterPartitionId,
                                                ReplicaIdType sourceReplicaId,
                                                TContext& context,
                                                TLogger& logger,
                                                const TClientDependencyTimestamp& dependencyTimestamp)
                    {
                        (new TSelf(neighbourPartitionServer,
                                   link,
                                   requesterPartitionId,
                                   sourceReplicaId,
                                   context,
                                   logger,
                                   dependencyTimestamp))->Next();
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
                                m_neighbourPartitionServer.ReceiveNeighbourGlobalTimestampDependencyCheckRequest(m_requesterPartitionId,
                                                                                                                 m_dependencyTimestamp,
                                                                                                                 m_sourceReplicaId);
                                delete this;
                            }break;
                                
                            default:
                            {
                                SR_ASSERT(false);
                            }break;
                        }
                    }
                    
                private:
                    OrbeDelayedInstallNeighbourPartitionGlobalDependencyCheckOperationState(TPartitionServer& neighbourPartitionServer,
                                                                                            TNetworkLink& link,
                                                                                            PartitionIdType requesterPartitionId,
                                                                                            ReplicaIdType sourceReplicaId,
                                                                                            TContext& context,
                                                                                            TLogger& logger,
                                                                                            const TClientDependencyTimestamp& dependencyTimestamp)
                    : m_neighbourPartitionServer(neighbourPartitionServer)
                    , m_link(link)
                    , m_requesterPartitionId(requesterPartitionId)
                    , m_sourceReplicaId(sourceReplicaId)
                    , m_context(context)
                    , m_logger(logger)
                    , m_dependencyTimestamp(dependencyTimestamp)
                    , m_phase(0)
                    {
                        
                    }
                    
                    TPartitionServer& m_neighbourPartitionServer;
                    TNetworkLink& m_link;
                    PartitionIdType m_requesterPartitionId;
                    ReplicaIdType m_sourceReplicaId;
                    TContext& m_context;
                    TLogger& m_logger;
                    TClientDependencyTimestamp m_dependencyTimestamp;
                    size_t m_phase;
                };
            }
        }
    }
}

