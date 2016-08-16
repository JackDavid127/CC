//
//  OrbeDelayedInstallVersionVectorParser.h
//  SimRunner
//
//  Created by Scott on 20/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "Wire.h"
#include "VersionVector.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            namespace Deployment
            {
                namespace Wire
                {
                    template <typename ProtocolTraits, typename TBufferReader, typename TVersionVector = typename ProtocolTraits::TVersionVector>
                    TVersionVector ReadVersionVector(TBufferReader& reader)
                    {
                        typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                        
                        const ReplicaIdType replicas(reader.template Read<ReplicaIdType>());
                        
                        TVersionVector vector;
                        
                        for(auto r = 0; r < replicas; ++ r)
                        {
                            TLogicalTimestamp logicalTimestamp(reader.template Read<TLogicalTimestamp>());
                            vector.SetValueForReplica(r, logicalTimestamp);
                        }
                        
                        return vector;
                    }
                    
                    template <typename ProtocolTraits, typename TBufferWriter, typename TVersionVector = typename ProtocolTraits::TVersionVector>
                    void WriteVersionVector(const TVersionVector& versionVector, TBufferWriter& writer)
                    {
                        typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                        
                        const ReplicaIdType replicas(versionVector.NumReplicas());
                        
                        writer.template Write<ReplicaIdType>(replicas);
                        
                        for(auto r = 0; r < replicas; ++ r)
                        {
                            writer.template Write<TLogicalTimestamp>(versionVector.GetValueForReplica(r));
                        }
                    }
                }
            }
        }
    }
}
