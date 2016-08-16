//
//  OrbeNeighbourPartitionDependencyCheckOperationState.h
//  SimRunner
//
//  Created by Scott on 01/11/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <memory>
#include <boost/noncopyable.hpp>
#include "Orbe.h"
#include "SimRunnerAssert.h"
#include "OrbePartitionServerReplicationHandler.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            namespace SimulatedComponents
            {
                template<typename TProtocolTraits, typename TCompletionHandler>
                class OrbeNeighbourPartitionDependencyCheckOperationState : private boost::noncopyable
                {
                    typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                    typedef typename TProtocolTraits::TNetworkLink TNetworkLink;
                    typedef typename TProtocolTraits::TVersionVector TVersionVector;
                    typedef typename TProtocolTraits::TNetworkExchange TContext;
                    typedef typename TProtocolTraits::TLogger TLogger;
                    
                    typedef OrbeNeighbourPartitionDependencyCheckOperationState<TProtocolTraits, TCompletionHandler> TSelf;
                    
                public:
                    static void DispatchRequest(TPartitionServer& neighbourPartitionServer,
                                                TNetworkLink& link,
                                                PartitionIdType requesterPartitionId,
                                                ReplicaIdType sourceReplicaId,
                                                TContext& context,
                                                TLogger& logger,
                                                const TVersionVector& vectorToVerify)
                    {
                        (new TSelf(neighbourPartitionServer,
                                   link,
                                   requesterPartitionId,
                                   sourceReplicaId,
                                   context,
                                   logger,
                                   vectorToVerify))->Next();
                    }
                    
                    void Next()
                    {
                        size_t currentPhase = m_phase++;
                        
                        switch(currentPhase)
                        {
                            case 0:
                            {
                                // send the message to the other partition server
                                m_link.SendInternalNetworkMessage(OrbeNetworkCompleteHandlerProxy<TContext>(m_context),
                                                                  OrbeNetworkOperationStateRequestCompleteHandlerProxy<TSelf>(*this));
                            }break;
                                
                            case 1:
                            {
                                // do the operation at the local partition server
                                m_neighbourPartitionServer.ReceiveNeighbourVersionVectorDependencyCheckRequest(m_requesterPartitionId,
                                                                                                               m_vectorToVerify,
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
                    OrbeNeighbourPartitionDependencyCheckOperationState(TPartitionServer& neighbourPartitionServer,
                                                                        TNetworkLink& link,
                                                                        PartitionIdType requesterPartitionId,
                                                                        ReplicaIdType sourceReplicaId,
                                                                        TContext& context,
                                                                        TLogger& logger,
                                                                        const TVersionVector& vectorToVerify)
                    : m_neighbourPartitionServer(neighbourPartitionServer)
                    , m_link(link)
                    , m_requesterPartitionId(requesterPartitionId)
                    , m_sourceReplicaId(sourceReplicaId)
                    , m_context(context)
                    , m_logger(logger)
                    , m_vectorToVerify(vectorToVerify)
                    , m_phase(0)
                    {
                        
                    }
                    
                    TPartitionServer& m_neighbourPartitionServer;
                    TNetworkLink& m_link;
                    PartitionIdType m_requesterPartitionId;
                    ReplicaIdType m_sourceReplicaId;
                    TContext& m_context;
                    TLogger& m_logger;
                    TVersionVector m_vectorToVerify;
                    size_t m_phase;
                };
            }
        }
    }
}

