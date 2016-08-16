//
//  SimulatedStorageComponent.h
//  SimRunner
//
//  Created by Scott on 29/09/2014.
//
//

#pragma once

#include <memory>
#include <boost/function.hpp>
#include <boost/noncopyable.hpp>
#include <boost/bind.hpp>
#include "SimulationInfrastructure.h"

namespace SimRunner
{
    namespace Components
    {   
        template<typename TEventSchedule>
        class SimulatedNetworkLinkComponent : private boost::noncopyable
        {
        public:
            SimulatedNetworkLinkComponent(const std::string& linkName,
                                          TEventSchedule& schedule)
            : m_maxConcurrentOperations(0)
            , m_internalOperationRoundTripLatencyMicroseconds(150)
            , m_externalOperationRoundTripLatencyMicroseconds(120000)
            , m_linkName(linkName)
            , m_schedule(schedule)
            {
                
            }
            
            template <typename TResultHandler, typename TRequestMetaData>
            void SendExternalNetworkMessage(TResultHandler handler, TRequestMetaData requestMetaData)
            {
                SendNetworkMessage(handler, requestMetaData, m_externalOperationRoundTripLatencyMicroseconds, "Ethernet external network event");
            }
            
            template <typename TResultHandler, typename TRequestMetaData>
            void SendInternalNetworkMessage(TResultHandler handler, TRequestMetaData requestMetaData)
            {
                SendNetworkMessage(handler, requestMetaData, m_internalOperationRoundTripLatencyMicroseconds, "Ethernet internal network event");
            }
            
            template <typename TResultHandler, typename TRequestMetaData>
            void PerformMessageCompletion(TResultHandler& handler, TRequestMetaData& requestMetaData)
            {
                handler.HandleLinkOperationComplete(requestMetaData);
            }
            
            
        private:
            const size_t m_maxConcurrentOperations;
            const SimulationInfrastructure::SimulationTimeMicroSeconds m_internalOperationRoundTripLatencyMicroseconds;
            const SimulationInfrastructure::SimulationTimeMicroSeconds m_externalOperationRoundTripLatencyMicroseconds;
            
            std::string m_linkName;
            TEventSchedule& m_schedule;
            
            template <typename TResultHandler, typename TRequestMetaData>
            void SendNetworkMessage(TResultHandler& handler,
                                    TRequestMetaData& requestMetaData,
                                    const SimulationInfrastructure::SimulationTimeMicroSeconds& latency,
                                    const std::string& tag)
            {
                ///hack
                //handler.HandleLinkOperationComplete(requestMetaData);
                //return;
                ///hack
                
                typedef DeferredNetworkEvent<TResultHandler, TRequestMetaData> TEvent;
                boost::function<void ()> bound = boost::bind(&TEvent::Handle, std::shared_ptr<TEvent>(new TEvent(*this, handler, requestMetaData)));
                SimRunner::SimulationInfrastructure::SimulationEvent event = m_schedule.BuildSimulationEventTimeStampedNow(bound, tag);
                m_schedule.InsertSimulationEvent(event, latency);
            }
            
            template <typename TResultHandler, typename TRequestMetaData>
            class DeferredNetworkEvent : private boost::noncopyable
            {
                SimulatedNetworkLinkComponent<TEventSchedule>& m_component;
                TResultHandler m_eventHandler;
                TRequestMetaData m_requestMetaData;
                
            public:
                
                DeferredNetworkEvent(SimulatedNetworkLinkComponent<TEventSchedule>& component,
                                     TResultHandler& eventHandler,
                                     TRequestMetaData& requestMetaData)
                : m_component(component)
                , m_eventHandler(eventHandler)
                , m_requestMetaData(requestMetaData)
                {
                }
                
                void Handle()
                {
                    m_component.PerformMessageCompletion(m_eventHandler, m_requestMetaData);
                }
            };
        };
    }
}
