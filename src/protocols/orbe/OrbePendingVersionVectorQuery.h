//
//  OrbePendingVersionVectorQuery.h
//  SimRunner
//
//  Created by Scott on 01/11/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "SimRunnerAssert.h"
#include "Orbe.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            template<typename TProtocolTraits>
            class OrbePendingVersionVectorQuery
            {
                typedef typename TProtocolTraits::TVersionVector TVersionVector;
                
            public:
                OrbePendingVersionVectorQuery()
                :m_isPending(false)
                {
                    
                }
                
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
                
                const TVersionVector& Vector() const
                {
                    SR_ASSERT(IsPending());
                    return m_vector;
                }
                
                void SetPendingQuery(const TVersionVector& vector,
                                     PartitionIdType senderPartitionId,
                                     ReplicaIdType sourceReplicaId)
                {
                    SR_ASSERT(!IsPending());
                    m_vector = vector;
                    m_senderPartitionId = senderPartitionId;
                    m_sourceReplicaId = sourceReplicaId;
                    m_isPending = true;
                }
                
                bool CanStatisfy(const TVersionVector& vector) const
                {
                    SR_ASSERT(IsPending());
                    return (vector.GreaterOrEquals(m_vector));
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
                TVersionVector m_vector;
                PartitionIdType m_senderPartitionId;
                ReplicaIdType m_sourceReplicaId;
                bool m_isPending;
            };
        }
    }
}
