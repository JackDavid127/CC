//
//  ConnectionStats.h
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
#include "ConnectionStatsClientOperation.h"
#include "SerializedConnectionStats.h"
#include "Timing.h"

namespace SimRunner
{
    namespace Metrics
    {
        namespace Connection
        {
            template <typename ProtocolTraits>
            class ConnectionStats : private boost::noncopyable
            {
                typedef typename ProtocolTraits::TMetricsStatsWriter TMetricsStatsWriter;
                typedef Utilities::TTimestamp TTimestamp;
                typedef Utilities::TTimeDelta TTimeDelta;
                typedef Protocols::ClientIdType TClientIdType;
                
            public:
                ConnectionStats(TMetricsStatsWriter& statsWriter,
                                TClientIdType clientConnectionId,
                                double durationBetweenLogsSeconds)
                : m_statsWriter(statsWriter)
                , m_running(false)
                , m_previousWriteTime(0)
                , m_clientConnectionId(clientConnectionId)
                , m_writeCounter(0)
                , m_requestInProgress(false)
                , m_currentRequestStartTime(0)
                , m_durationBetweenLogsSeconds(durationBetweenLogsSeconds)
                {
                }
                
                void RequestStarted(bool requestIsGet)
                {
                    SR_ASSERT(!m_requestInProgress);
                    
                    if(!m_running)
                    {
                        m_previousWriteTime = Utilities::Now();
                        m_running = true;
                    }
                    
                    m_currentRequestIsGet = requestIsGet;
                    m_currentRequestStartTime = Utilities::Now();
                    m_requestInProgress = true;
                }
                
                void RequestCompleted()
                {
                    SR_ASSERT(m_requestInProgress);
                    
                    TTimestamp now(Utilities::Now());
                    
                    TTimeDelta timeDelta = Utilities::GetDeltaTime(now, m_currentRequestStartTime);
                    ConnectionStatsClientOperation clientOp(m_currentRequestIsGet, timeDelta);
                    m_latencies.push_back(clientOp);
                    
                    m_requestInProgress = false;
                    
                    TryFlush(now);
                }
                
            private:
                void TryFlush(TTimestamp& currentTimeNow)
                {
                    double secondsSincePreviousWrite = Utilities::GetDeltaTime<Utilities::Seconds>(currentTimeNow, m_previousWriteTime).count();

                    if(secondsSincePreviousWrite >= m_durationBetweenLogsSeconds)
                    {
                        SerializedConnectionStats stats(m_clientConnectionId, m_previousWriteTime, currentTimeNow, m_latencies);
                        m_statsWriter.WriteJson(m_clientConnectionId, m_writeCounter, stats.Json());
                        
                        ++m_writeCounter;
                        m_latencies.clear();
                        m_previousWriteTime = currentTimeNow;
                    }
                }
                
                TMetricsStatsWriter& m_statsWriter;
                bool m_running;
                TTimestamp m_previousWriteTime;
                TClientIdType m_clientConnectionId;
                size_t m_writeCounter;
                std::vector<ConnectionStatsClientOperation> m_latencies;
                double m_durationBetweenLogsSeconds;
                
                bool m_requestInProgress;
                bool m_currentRequestIsGet;
                TTimestamp m_currentRequestStartTime;
            };
        }
    }
}
