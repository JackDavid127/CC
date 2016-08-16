//
//  SimulationEventSchedule.h
//  SimRunner
//
//  Created by Scott on 24/09/2014.
//
//

#pragma once

#include "stddef.h"
#include <boost/function.hpp>
#include <boost/noncopyable.hpp>
#include <boost/heap/priority_queue.hpp>
#include "SimulationInfrastructure.h"
#include "Utilities.h"
#include "SimulationEvent.h"
#include "SimulationEventComparator.h"
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace SimulationInfrastructure
    {
        template <typename TSimulationClock, typename TLogger>
        class SimulationEventSchedule : private boost::noncopyable
        {
        public:
            SimulationEventSchedule(TSimulationClock& clock, TLogger& logger)
            : m_clock(clock)
            , m_logger(logger)
            , m_eventTag(0)
            , m_eventCompletedCounter(0)
            {
                
            }

            bool HasEvents() const
            {
                return m_schedule.size() != 0;
            }
            
            SimulationEvent BuildSimulationEventTimeStampedNow(boost::function<void ()>& eventCompletionCallback, const std::string& message="")
            {
                return SimulationEvent(eventCompletionCallback, m_clock.CurrentMicroSeconds(), message);
            }
            
            void InsertSimulationEvent(SimulationEvent& event, SimulationTimeMicroSeconds latency)
            {
                ++ m_eventTag;
                SimulationTimeMicroSeconds completionTime = m_clock.CurrentMicroSeconds() + latency;
                event.ScheduleForCompletion(completionTime, m_eventTag);
                m_schedule.push(event);
            }
            
            void Drain()
            {
                SimulationTimeMicroSeconds lastCompletionTime = 0;
                
                while(HasEvents())
                {
                    SimulationEvent event = m_schedule.top();
                    m_schedule.pop();
                    
                    const SimulationTimeMicroSeconds completionTime = event.CompletionTime();
                    
                    SR_ASSERT(!event.IsComplete());
                    SR_ASSERT(completionTime >= lastCompletionTime, "completion_time %s, last_completion_time %s", completionTime, lastCompletionTime);
                    
                    lastCompletionTime = completionTime;
                    ++ m_eventCompletedCounter;
                    m_clock.UpdateTime(completionTime);
                    event.Complete();
                }
            }
            
        private:
            TSimulationClock& m_clock;
            TLogger& m_logger;
            SimulationEventTag m_eventTag;
            size_t m_eventCompletedCounter;
            boost::heap::priority_queue<SimulationEvent, boost::heap::compare<SimulationEventComparator> > m_schedule;
        };
    }
}
