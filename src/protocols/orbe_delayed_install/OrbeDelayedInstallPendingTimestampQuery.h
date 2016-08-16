//
//  OrbeDelayedInstallPendingTimestampQuery.h
//  SimRunner
//
//  Created by Scott on 01/11/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "SimRunnerAssert.h"
#include "OrbeDelayedInstall.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            template<typename TProtocolTraits>
            class OrbeDelayedInstallPendingTimestampQuery
            {
                typedef typename TProtocolTraits::TVersionVector TVersionVector;
                typedef typename TProtocolTraits::TClientDependencyTimestamp TClientDependencyTimestamp;
                
            protected:
                OrbeDelayedInstallPendingTimestampQuery()
                :m_isPending(false)
                {
                    
                }
                
            public:
                
                PartitionIdType SenderPartitionId() const
                {
                    SR_ASSERT(IsPending());
                    return m_senderPartitionId;
                }
                
                ReplicaIdType SourceReplicaId() const
                {
                    SR_ASSERT(IsPending());
                    return m_sourceReplicaId;
                }
                
                TClientDependencyTimestamp DependencyTimestamp() const
                {
                    SR_ASSERT(IsPending());
                    return m_dependencyTimestamp;
                }
                
                void SetPendingQuery(const TClientDependencyTimestamp& dependencyTimestamp,
                                     PartitionIdType senderPartitionId,
                                     ReplicaIdType sourceReplicaId)
                {
                    SR_ASSERT(!IsPending());
                    m_dependencyTimestamp = dependencyTimestamp;
                    m_senderPartitionId = senderPartitionId;
                    m_sourceReplicaId = sourceReplicaId;
                    m_isPending = true;
                }
                
                bool CanStatisfy(const TVersionVector& vector) const
                {
                    SR_ASSERT(IsPending());
                    return (vector.GetValueForReplica(m_sourceReplicaId) >= m_dependencyTimestamp);
                }
                
                void Satisfy()
                {
                    SR_ASSERT(IsPending());
                    m_isPending = false;
                }
                
                bool IsPending() const
                {
                    return m_isPending;
                }
                
            private:
                TClientDependencyTimestamp m_dependencyTimestamp;
                PartitionIdType m_senderPartitionId;
                ReplicaIdType m_sourceReplicaId;
                bool m_isPending;
            };
            
            template<typename TProtocolTraits>
            class OrbeDelayedInstallPendingLocalTimestampQuery : public OrbeDelayedInstallPendingTimestampQuery<TProtocolTraits> { };
            
            template<typename TProtocolTraits>
            class OrbeDelayedInstallPendingGlobalTimestampQuery : public OrbeDelayedInstallPendingTimestampQuery<TProtocolTraits> { };
        }
    }
}
