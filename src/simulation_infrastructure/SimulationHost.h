//
//  SimulationHost.h
//  SimRunner
//
//  Created by Scott on 25/09/2014.
//
//

#pragma once

#include <stddef.h>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <boost/noncopyable.hpp>
#include "SimulationInfrastructure.h"
#include "Utilities.h"
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace SimulationInfrastructure
    {
        template <typename TSimulationProtocol, typename TEventSchedule, typename TLogger>
        class SimulationHost : private boost::noncopyable
        {
        public:
            SimulationHost(TSimulationProtocol& protocol,
                           size_t numOperations,
                           SimulationTimeMicroSeconds interOperationalEventDelay,
                           TEventSchedule& schedule,
                           TLogger& logger)
            : m_protocol(protocol)
            , m_numOperations(numOperations)
            , m_interOperationalEventDelay(interOperationalEventDelay)
            , m_schedule(schedule)
            , m_logger(logger)
            , m_operationCount(0)
            , m_eventOccurenceCallback(boost::bind(&SimulationHost::AddNextOperationEvent, this))
            {
                SR_ASSERT(m_interOperationalEventDelay >= 0);
            }
            
            void Run()
            {
                SimulationEvent firstClientEvent = m_schedule.BuildSimulationEventTimeStampedNow(m_eventOccurenceCallback, "Client operation event!");
                m_schedule.InsertSimulationEvent(firstClientEvent, m_interOperationalEventDelay);
                m_schedule.Drain();
                m_protocol.End();
            }
            
        private:
            bool AllEventsDispatched() const { return m_operationCount == m_numOperations; }
            
            void AddNextOperationEvent()
            {
                m_protocol.PerformOperation();
                
                SR_ASSERT(!AllEventsDispatched());
                
                m_operationCount += 1;
                LogUpdateMessage(m_operationCount);
                
                if(!AllEventsDispatched())
                {
                    SimulationEvent nextEvent = m_schedule.BuildSimulationEventTimeStampedNow(m_eventOccurenceCallback, "Client operation event!");
                    m_schedule.InsertSimulationEvent(nextEvent, m_interOperationalEventDelay);
                }
            }
            
            void LogUpdateMessage(size_t messageNumber)
            {
                if((messageNumber % 10000) == 0)
                {
                    m_logger.Log("%d / %d operations -- %f%% complete...",
                                 messageNumber,
                                 m_numOperations,
                                 (messageNumber/m_numOperations)*100.f);
                }
            }
            
            TSimulationProtocol& m_protocol;
            size_t m_numOperations;
            SimulationTimeMicroSeconds m_interOperationalEventDelay;
            TEventSchedule& m_schedule;
            TLogger& m_logger;
            size_t m_operationCount;
            boost::function<void ()> m_eventOccurenceCallback;
        };
    }
}
