//
//  OrbeDelayedInstallNeighbourPartitionDependencyResponseOperationState.h
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
                class OrbeDelayedInstallNeighbourPartitionDependencyResponseOperationState : private boost::noncopyable
                {
                    typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                    typedef typename TProtocolTraits::TNetworkLink TNetworkLink;
                    typedef typename TProtocolTraits::TVersionVector TVersionVector;
                    typedef typename TProtocolTraits::TNetworkExchange TContext;
                    typedef typename TProtocolTraits::TLogger TLogger;
                    
                    typedef OrbeDelayedInstallNeighbourPartitionDependencyResponseOperationState<TProtocolTraits, TCompletionHandler> TSelf;
                    
                public:
                    static void DispatchRequest(TPartitionServer& neighbourPartitionServer,
                                                TNetworkLink& link,
                                                ReplicaIdType sourceReplicaId,
                                                TContext& context,
                                                TLogger& logger)
                    {
                        (new TSelf(neighbourPartitionServer,
                                   link,
                                   sourceReplicaId,
                                   context,
                                   logger))->Next();
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
                                m_neighbourPartitionServer.ReceiveNeighbourTimestampDependencyCheckResponse(m_sourceReplicaId);
                                delete this;
                            }break;
                                
                            default:
                            {
                                SR_ASSERT(false);
                            }break;
                        }
                    }
                    
                private:
                    OrbeDelayedInstallNeighbourPartitionDependencyResponseOperationState(TPartitionServer& neighbourPartitionServer,
                                                                                         TNetworkLink& link,
                                                                                         ReplicaIdType sourceReplicaId,
                                                                                         TContext& context,
                                                                                         TLogger& logger)
                    : m_neighbourPartitionServer(neighbourPartitionServer)
                    , m_link(link)
                    , m_sourceReplicaId(sourceReplicaId)
                    , m_context(context)
                    , m_logger(logger)
                    , m_phase(0)
                    {
                        
                    }
                    
                    TPartitionServer& m_neighbourPartitionServer;
                    TNetworkLink& m_link;
                    ReplicaIdType m_sourceReplicaId;
                    TContext& m_context;
                    TLogger& m_logger;
                    size_t m_phase;
                };
            }
        }
    }
}

