//
//  DependencyMatrix.h
//  SimRunner
//
//  Created by Scott on 27/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <boost/noncopyable.hpp>
#include <vector>
#include "VersionVector.h"
#include "Orbe.h"

#include <string>
#include <sstream>
#include "VersionVectorString.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            template <PartitionIdType Partitions, ReplicaIdType Replicas, typename TValueType>
            class DependencyMatrix// : private boost::noncopyable
            {
            public:
                typedef DependencyMatrix<Partitions, Replicas, TValueType> TSelf;
                typedef Utilities::VersionVector<Replicas, TValueType> TVec;
                
                DependencyMatrix()
                : m_entries()
                {
                    
                }
                
                constexpr ReplicaIdType NumReplicas() const
                {
                    return Replicas;
                }
                
                constexpr PartitionIdType NumPartitions() const
                {
                    return Partitions;
                }
                
                PartitionIdType GetPartitionId(size_t index) const
                {
                    return index;
                }
                
                void Update(PartitionIdType partitionServerId,
                            ReplicaIdType replicaId,
                            const TValueType& logicalTimestamp)
                {
                    TVec& partitionReplicaData = m_entries[partitionServerId];
                    TryUpdateReplicaTimestamp(replicaId, logicalTimestamp, partitionReplicaData);
                }
                
                bool IsEmpty(PartitionIdType partitionServerId) const
                {
                    return m_entries[partitionServerId].IsEmpty();
                }
                
                void ComputeVersionVector(PartitionIdType partitionServerId,
                                          TVec& out_versionVector) const
                {
                    const TVec& versionVector(m_entries[partitionServerId]);
                    
                    for(ReplicaIdType r = 0; r < Replicas; ++ r)
                    {
                        TValueType value = versionVector.GetValueForReplica(r);
                        out_versionVector.SetValueForReplica(r, value);
                    }
                }
                
                void Reset()
                {
                    for(PartitionIdType p = 0; p < Partitions; ++ p)
                    {
                        m_entries[p].Reset();
                    }
                }
                
                std::string ToString() const
                {
                    std::stringstream ss;
                    
                    ss << "\n";
                    
                    for(PartitionIdType p = 0; p < Partitions; ++ p)
                    {
                        const auto& vec(m_entries[p]);
                        ss << "\tP: " << p << " vector: " << Utilities::ToString(vec) << "\n";
                    }
                    
                    return ss.str();
                }
                
            private:
                const TValueType GetElement(PartitionIdType partitionServerId,
                                            ReplicaIdType replicaId) const
                {
                    const TVec& versionVector(m_entries[partitionServerId]);
                    return versionVector[replicaId];
                }
                
                void TryUpdateReplicaTimestamp(ReplicaIdType sourceReplicaId,
                                               const TValueType& logicalTimestamp,
                                               TVec& partitionServerData) const
                {
                    const TValueType& currentValue = partitionServerData.GetValueForReplica(sourceReplicaId);
                    
                    if(logicalTimestamp > currentValue)
                    {
                        partitionServerData.SetValueForReplica(sourceReplicaId, logicalTimestamp);
                    }
                }
                
                TVec m_entries[Partitions];
            };
        }
    }
}
