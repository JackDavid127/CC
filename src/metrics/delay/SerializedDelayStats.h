//
//  SerializedDelayStats.h
//  SimRunner
//
//  Created by Scott on 12/01/2015.
//  Copyright (c) 2015 Scott. All rights reserved.
//

#pragma once

#include <sstream>
#include <string>
#include "document.h"
#include "writer.h"
#include "stringbuffer.h"
#include "Protocols.h"
#include "Timing.h"
#include "DelayStatsOperation.h"

namespace SimRunner
{
    namespace Metrics
    {
        namespace Delay
        {
            class SerializedDelayStats
            {
                typedef Utilities::TTimestamp TTimeStamp;
                typedef Protocols::ClientIdType TClientIdType;
                
            public:
                SerializedDelayStats(const std::string& serverId,
                                     TTimeStamp previousWriteTime,
                                     TTimeStamp timeNow,
                                     const std::vector<DelayStatsOperation>& latencies)
                {
                    
                    double samplePeriodSeconds(Utilities::GetDeltaTime(timeNow, previousWriteTime).count() * 1e-6);
                    uint32_t sampleCount(static_cast<uint32_t>(latencies.size()));
                    
                    rapidjson::Document root;
                    rapidjson::Document::AllocatorType& allocator = root.GetAllocator();
                    
                    root.SetObject();
                    
                    root.AddMember("ServerId", serverId.c_str(), allocator);
                    root.AddMember("StartPeriod", previousWriteTime.count(), allocator);
                    root.AddMember("EndPeriod", timeNow.count(), allocator);
                    root.AddMember("PeriodLength", samplePeriodSeconds, allocator);
                    root.AddMember<uint32_t>("Samples", sampleCount, allocator);
                    root.AddMember("OpsPerSecond", static_cast<double>(sampleCount/samplePeriodSeconds), allocator);
                    
                    rapidjson::Value samples(rapidjson::kArrayType);
                    samples.Reserve(sampleCount, allocator);
                    
                    for(std::vector<DelayStatsOperation>::const_iterator it = latencies.begin(); it != latencies.end(); ++ it)
                    {
                        const DelayStatsOperation& op(*it);
                        
                        rapidjson::Value operationJson(rapidjson::kArrayType);
                        
                        rapidjson::Value type(op.WriteVisibilityLatency());
                        operationJson.PushBack(type, allocator);
                        
                        rapidjson::Value value(static_cast<int16_t>(op.NumberOfPartitionDependencies()));
                        operationJson.PushBack(value, allocator);
                        
                        samples.PushBack(operationJson, allocator);
                    }
                    
                    root.AddMember("Values", samples, allocator);
                    
                    rapidjson::StringBuffer strbuf;
                    rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
                    root.Accept(writer);
                    m_json = strbuf.GetString();
                }
                
                const std::string& Json() const
                {
                    return m_json;
                }
                
            private:
                std::string m_json;
            };
        }
    }
}
