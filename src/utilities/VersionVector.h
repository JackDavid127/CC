//
//  VersionVector.h
//  SimRunner
//
//  Created by Scott on 27/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <cstdlib>
#include <vector>
#include <boost/noncopyable.hpp>
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace Utilities
    {
        template <Protocols::ReplicaIdType Replicas, typename TCounterType>
        class VersionVector// : private boost::noncopyable
        {
            typedef VersionVector<Replicas, TCounterType> TSelf;
            typedef Protocols::ReplicaIdType ReplicaIdType;
            
        public:
            typedef TCounterType TVersionVectorCounterType;
            
            VersionVector()
            : m_data()
            {
                
            }
            
            static constexpr size_t SerializationSize()
            {
                //fields + num of fields
                return sizeof(TSelf) + sizeof(Protocols::ReplicaIdType);
            }
            
            Protocols::ReplicaIdType NumReplicas() const
            {
                return Replicas;
            }
            
            bool IsEmpty() const
            {
                for(ReplicaIdType r = 0; r < Replicas; ++ r)
                {
                    if(m_data[r] != 0)
                    {
                        return false;
                    }
                }
                
                return true;
            }
            
            TCounterType IncrementForReplica(ReplicaIdType replicaId)
            {
                return ++(m_data[replicaId]);
            }
            
            void SetValueForReplica(ReplicaIdType replicaId, const TCounterType& value)
            {
                m_data[replicaId] = value;
            }
            
            TCounterType GetValueForReplica(ReplicaIdType replicaId) const
            {
                return m_data[replicaId];
            }
        
            void Reset()
            {
                for(ReplicaIdType r = 0; r < Replicas; ++ r)
                {
                    m_data[r] = TCounterType();
                }
            }
            
            bool GreaterOrEquals(const TSelf& otherVersionVector) const
            {
                for(ReplicaIdType r = 0; r < Replicas; ++ r)
                {
                    if(GetValueForReplica(r) < otherVersionVector.GetValueForReplica(r))
                    {
                        return false;
                    }
                }
                        
                return true;
            }
            
        private:
            TCounterType m_data[Replicas];
        };
    }
}
