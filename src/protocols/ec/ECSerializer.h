//
//  ECSerializer.h
//  SimRunner
//
//  Created by Scott on 05/10/2014.
//
//

#pragma once

#include <string>
#include <memory>
#include <vector>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include "EC.h"
#include "SimRunnerAssert.h"
#include "Timing.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace EC
        {
            template<
            typename TSerializedDataType,
            typename TDeserializedValueType
            >
            class ECSerializer
            {
            public:
                TSerializedDataType Serialize(const TDeserializedValueType& item) const
                {
                    /*std::string keyStr = boost::lexical_cast<std::string>(item.Key());
                    std::string valueStr = boost::lexical_cast<std::string>(item.Value());
                    std::string sourceReplicaStr = boost::lexical_cast<std::string>(item.SourceReplicaId());
                    
                    boost::int64_t count = item.Timestamp().count();
                    std::string timestampStr = boost::lexical_cast<std::string>(count);
                    
                    return keyStr + "," + valueStr + "," + sourceReplicaStr + "," + timestampStr;*/
                    return item.ToData();
                }
                
                TDeserializedValueType Deserialize(const TSerializedDataType& serialized) const
                {
                    /*std::vector<std::string> output;
                    boost::split(output, serialized, boost::is_any_of(","));
                    SR_ASSERT(output.size() == 4);
                    
                    std::string keyStr(output[0]);
                    std::string valueStr(output[1]);
                    std::string sourceReplicaStr(output[2]);
                    std::string timestampStr(output[3]);
                    
                    int key = boost::lexical_cast<int>(keyStr);
                    int value = boost::lexical_cast<int>(valueStr);
                    ReplicaIdType sourceReplica = boost::lexical_cast<ReplicaIdType>(sourceReplicaStr);
                    
                    boost::int64_t micros = boost::lexical_cast<boost::int64_t>(timestampStr);
                    Utilities::TTimestamp timestamp(micros);
                    
                    return TDeserializedValueType(key, value, timestamp, sourceReplica);*/
                    return TDeserializedValueType::FromData(serialized);
                }
            };
        }
    }
}
