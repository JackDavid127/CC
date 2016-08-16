//
//  SerializedConnectionStats.h
//  SimRunner
//
//  Created by Scott on 25/11/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <sstream>
#include <string>
#include "document.h"
#include "writer.h"
#include "stringbuffer.h"
#include "Protocols.h"
#include "Timing.h"
#include "ConnectionStatsClientOperation.h"

namespace SimRunner
{
    namespace Metrics
    {
        namespace Connection
        {
            class SerializedConnectionStats
            {
                typedef Utilities::TTimestamp TTimeStamp;
                typedef Protocols::ClientIdType TClientIdType;
                
            public:
                SerializedConnectionStats(TClientIdType clientConnectionId,
                                          TTimeStamp previousWriteTime,
                                          TTimeStamp timeNow,
                                          const std::vector<ConnectionStatsClientOperation>& latencies)
                {
                    
                    double samplePeriodSeconds(Utilities::GetDeltaTime(timeNow, previousWriteTime).count() * 1e-6);
                    uint32_t sampleCount(static_cast<uint32_t>(latencies.size()));
                    
                    rapidjson::Document root;
                    rapidjson::Document::AllocatorType& allocator = root.GetAllocator();
                    
                    root.SetObject();
                    
                    root.AddMember("ConnectionId", clientConnectionId, allocator);
                    root.AddMember("StartPeriod", previousWriteTime.count(), allocator);
                    root.AddMember("EndPeriod", timeNow.count(), allocator);
                    root.AddMember("PeriodLength", samplePeriodSeconds, allocator);
                    root.AddMember<uint32_t>("Samples", sampleCount, allocator);
                    root.AddMember("OpsPerSecond", static_cast<double>(sampleCount/samplePeriodSeconds), allocator);
                    
                    rapidjson::Value samples(rapidjson::kArrayType);
                    samples.Reserve(sampleCount, allocator);
                    
                    for(std::vector<ConnectionStatsClientOperation>::const_iterator it = latencies.begin(); it != latencies.end(); ++ it)
                    {
                        const ConnectionStatsClientOperation& op(*it);
                        
                        rapidjson::Value operationJson(rapidjson::kArrayType);
                       
                        rapidjson::Value type(op.RequestIsGet() ? "g" : "p");
                        operationJson.PushBack(type, allocator);
                        
                        rapidjson::Value value(op.RequestDurationSeconds());
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
