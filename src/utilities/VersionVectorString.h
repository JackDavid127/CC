//
//  VersionVectorString.h
//  SimRunner
//
//  Created by Scott on 10/11/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <sstream>
#include <iostream>
#include "SimRunnerAssert.h"
#include "VersionVector.h"

namespace SimRunner
{
    namespace Utilities
    {
        template <Protocols::ReplicaIdType Replicas, typename TCounterType>
        std::ostream& operator << (std::ostream& stream, const VersionVector<Replicas, TCounterType>& vector)
        {
            stream << "VersionVector: [ " << vector.GetValueForReplica(0);
            
            for(Protocols::ReplicaIdType r = 1; r < vector.NumReplicas(); ++ r)
            {
                stream << ", " << vector.GetValueForReplica(r);
            }
            
            stream << "]";
            
            return stream;
        }
        
        template<typename TVersionVector>
        std::string ToString(const TVersionVector& vector)
        {
            std::stringstream ss;
            ss << vector;
            return ss.str();
        }
    }
}

