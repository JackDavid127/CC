//
//  SimulationEvent.h
//  SimRunner
//
//  Created by Scott on 24/09/2014.
//
//

#pragma once

#include <string>
#include <boost/function.hpp>
#include "SimulationInfrastructure.h"
#include "SimulationTime.h"
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace SimulationInfrastructure
    {
        class SimulationEvent
        {
        public:
            SimulationEvent(boost::function<void ()>& eventCompletionCallback,
                                             SimulationTimeMicroSeconds beginTimestamp,
                                             const std::string& message)
            : m_complete(false)
            , m_scheduled(false)
            , m_message(message)
            , m_beginTimestamp(beginTimestamp)
            , m_callback(eventCompletionCallback)
            {
                SR_ASSERT(beginTimestamp >= 0);
            }
            
            SimulationTimeMicroSeconds OccurrenceTime() const
            {
                return m_beginTimestamp;
            }
            
            bool IsScheduled() const
            {
                return m_scheduled;
            }
            
            bool IsComplete() const
            {
                return m_complete;
            }
            
            std::string ToString() const
            {
                return m_message;
            }
            
            void Complete()
            {
                SR_ASSERT(m_scheduled);
                SR_ASSERT(!m_complete);
                
                m_complete = true;
                m_callback();
            }
            
            SimulationTimeMicroSeconds CompletionTime() const
            {
                SR_ASSERT(m_scheduled);
                return m_completionTime;
            }
            
            SimulationEventTag EventTag() const
            {
                SR_ASSERT(m_scheduled);
                return m_tag;
            }
            
            void ScheduleForCompletion(SimulationTimeMicroSeconds completionTime, SimulationEventTag tag)
            {
                SR_ASSERT(!m_scheduled);
                SR_ASSERT(!m_complete);
                SR_ASSERT(completionTime >= m_beginTimestamp);
                
                m_scheduled = true;
                m_completionTime = completionTime;
                m_tag = tag;
            }
            
            SimulationTimeMicroSeconds Latency() const
            {
                SR_ASSERT(m_complete);
                return m_completionTime - OccurrenceTime();
            }
            
        private:
            bool m_scheduled;
            bool m_complete;
            boost::function<void ()> m_callback;
            SimulationTimeMicroSeconds m_beginTimestamp;
            SimulationTimeMicroSeconds m_completionTime;
            SimulationEventTag m_tag;
            std::string m_message;
        };
    }
}
