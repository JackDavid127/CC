//
//  OrbeDependencyMatrixParser.h
//  SimRunner
//
//  Created by Scott on 20/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "Wire.h"
#include "DependencyMatrix.h"
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            namespace Deployment
            {
                namespace Wire
                {
                    template <typename ProtocolTraits, typename TDependencyMatrix = typename ProtocolTraits::TDependencyMatrix>
                    size_t ComputeDependencyMatrixSerializationSize(const TDependencyMatrix& dependencyMatrix)
                    {
                        typedef typename ProtocolTraits::TVersionVector TVersionVector;
                        typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                        
                        const PartitionIdType partitions(dependencyMatrix.NumPartitions());
                        const ReplicaIdType replicas(dependencyMatrix.NumReplicas());
                        
                        size_t totalNonZeroElementsSizeBytes(0);
                        
                        for(auto p = 0; p < partitions; ++ p)
                        {
                            TVersionVector out_versionVector;
                            dependencyMatrix.ComputeVersionVector(p, out_versionVector);
                            
                            for(auto r = 0; r < replicas; ++ r)
                            {
                                const auto valueForReplica(out_versionVector.GetValueForReplica(r));
                                
                                if(valueForReplica != 0)
                                {
                                    totalNonZeroElementsSizeBytes += sizeof(valueForReplica);
                                }
                            }
                        }
                        
                        return (sizeof(uint8_t) * partitions) + totalNonZeroElementsSizeBytes;
                    }
                    
                    template <
                    typename ProtocolTraits,
                    typename TBufferReader,
                    typename TDependencyMatrix = typename ProtocolTraits::TDependencyMatrix>
                    TDependencyMatrix ReadDependencyMatrix(TBufferReader& reader)
                    {
                        typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                        
                        TDependencyMatrix matrix;
                        
                        const PartitionIdType partitions(matrix.NumPartitions());
                        const ReplicaIdType replicas(matrix.NumReplicas());
                        
                        for(auto p = 0; p < partitions; ++ p)
                        {
                            const uint8_t partitionMask(reader.template Read<uint8_t>());
                            
                            for(auto r = 0; r < replicas; ++ r)
                            {
                                if((partitionMask & (1 << r)) != 0)
                                {
                                    TLogicalTimestamp logicalTimestamp(reader.template Read<TLogicalTimestamp>());
                                    matrix.Update(p, r, logicalTimestamp);
                                }
                            }
                        }
                        
                        return matrix;
                    }
                    
                    template <typename ProtocolTraits, typename TBufferWriter, typename TDependencyMatrix = typename ProtocolTraits::TDependencyMatrix>
                    void WriteDependencyMatrix(const TDependencyMatrix& dependencyMatrix, TBufferWriter& writer)
                    {
                        typedef typename ProtocolTraits::TVersionVector TVersionVector;
                        typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                        
                        const PartitionIdType partitions(dependencyMatrix.NumPartitions());
                        const ReplicaIdType replicas(dependencyMatrix.NumReplicas());
                        
                        SR_ASSERT(partitions >= 1 && partitions <= 8,
                                  "DependencyMatrix serialization method only supports between 1 and 8 partitions.");
                        
                        for(auto p = 0; p < partitions; ++ p)
                        {
                            TVersionVector out_versionVector;
                            dependencyMatrix.ComputeVersionVector(p, out_versionVector);
                            uint8_t partitionMask = 0x0;
                            
                            for(auto r = 0; r < replicas; ++ r)
                            {
                                const auto valueForReplica(out_versionVector.GetValueForReplica(r));
                                
                                if(valueForReplica != 0)
                                {
                                    partitionMask |= (1 << r);
                                }
                            }
                            
                            writer.template Write<uint8_t>(partitionMask);
                            
                            for(auto r = 0; r < replicas; ++ r)
                            {
                                const auto valueForReplica(out_versionVector.GetValueForReplica(r));
                                
                                if(valueForReplica != 0)
                                {
                                    writer.template Write<TLogicalTimestamp>(out_versionVector.GetValueForReplica(r));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
