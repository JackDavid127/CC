//
//  DelayStats.h
//  SimRunner
//
//  Created by Scott on 25/11/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <string>
#include <vector>
#include <boost/noncopyable.hpp>
#include "Protocols.h"
#include "DelayStatsOperation.h"
#include "SerializedDelayStats.h"
#include "Timing.h"

namespace SimRunner
{
    namespace Metrics
    {
        namespace Delay
        {
            template <typename ProtocolTraits>
            class DelayStats : private boost::noncopyable
            {
                typedef typename ProtocolTraits::TMetricsStatsWriter TMetricsStatsWriter;
                typedef typename ProtocolTraits::TDeserializedValueType TDeserializedValueType;
                
                typedef Utilities::TTimestamp TTimestamp;
                typedef Utilities::TTimeDelta TTimeDelta;
                
            public:
                DelayStats(TMetricsStatsWriter& statsWriter,
                           const std::string& serverId,
                           double durationBetweenLogsSeconds)
                : m_statsWriter(statsWriter)
                , m_previousWriteTime(Utilities::Now())
                , m_serverId(serverId)
                , m_writeCounter(0)
                , m_durationBetweenLogsSeconds(durationBetweenLogsSeconds)
                {
                }
                
                void OperationPerformed(const TTimeDelta& writeVisibilityLatency,
                                        size_t numOtherPartitionsDepended=0)
                {
                    DelayStatsOperation op = DelayStatsOperation(numOtherPartitionsDepended,
                                                                 writeVisibilityLatency);
                    m_operations.push_back(op);
                    TTimestamp now(Utilities::Now());
                    TryFlush(now);
                }
                
            private:
                void TryFlush(TTimestamp& currentTimeNow)
                {
                    double secondsSincePreviousWrite = Utilities::GetDeltaTime<Utilities::Seconds>(currentTimeNow, m_previousWriteTime).count();
                    
                    if(secondsSincePreviousWrite >= m_durationBetweenLogsSeconds)
                    {
                        SerializedDelayStats stats(m_serverId, m_previousWriteTime, currentTimeNow, m_operations);
                        m_statsWriter.WriteJson(m_serverId, m_writeCounter++, stats.Json());
                        
                        m_operations.clear();
                        m_previousWriteTime = currentTimeNow;
                    }
                }
                
                TMetricsStatsWriter& m_statsWriter;
                TTimestamp m_previousWriteTime;
                std::string m_serverId;
                size_t m_writeCounter;
                std::vector<DelayStatsOperation> m_operations;
                double m_durationBetweenLogsSeconds;
            };
        }
        
    }
}
