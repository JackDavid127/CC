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
#include "KeyValueStorage.h"

namespace SimRunner
{
    namespace Components
    {
        template<typename TProxiedPutCompleteHandlerObject>
        class PutCompleteHandlerProxy
        {
            typedef void* PlaceholderContext;
            static constexpr void* EmptyContext = nullptr;
        public:
            PutCompleteHandlerProxy(TProxiedPutCompleteHandlerObject& proxy)
            : m_pProxy(&proxy) { }
            
            operator TProxiedPutCompleteHandlerObject&() const
            {
                return *m_pProxy;
            }
            
            template<typename TClientPutReplyValueType>
            void HandlePutComplete(const TClientPutReplyValueType& value, PlaceholderContext = EmptyContext)
            {
                m_pProxy->HandlePutComplete(value);
            }
            
            template<typename TPutCompletedHandler, typename TDeserializedValueType>
            void HandlePutComplete(TPutCompletedHandler putCompletedHandler, const TDeserializedValueType& protocolItem)
            {
                m_pProxy->HandlePutComplete(putCompletedHandler, protocolItem, EmptyContext);
            }
            
        private:
            TProxiedPutCompleteHandlerObject* m_pProxy;
        };
        
        template<typename TProxiedGetCompleteHandlerObject>
        class GetCompleteHandlerProxy
        {
            typedef void* PlaceholderContext;
            static constexpr void* EmptyContext = nullptr;
        public:
            GetCompleteHandlerProxy(TProxiedGetCompleteHandlerObject& proxy)
            : m_pProxy(&proxy) { }
            
            template<typename TClientGetReplyValueType>
            void HandleGetCompleteItemFound(const TClientGetReplyValueType& value, PlaceholderContext = EmptyContext)
            {
                m_pProxy->HandleGetCompleteItemFound(value);
            }
            
            void HandleGetCompleteItemNotFound(PlaceholderContext = EmptyContext)
            {
                m_pProxy->HandleGetCompleteItemNotFound();
            }
            
            operator TProxiedGetCompleteHandlerObject&() const
            {
                return *m_pProxy;
            }
            
            template<typename TGetCompletedHandler, typename TClientGetReplyValueType, typename TAdditionalContext>
            void HandleGetCompleteItemFound(TGetCompletedHandler getCompletedHandler,
                                            const TClientGetReplyValueType& value,
                                            TAdditionalContext additionalContext)
            {
                m_pProxy->HandleGetCompleteItemFound(getCompletedHandler, value, additionalContext);
            }
            
            template<typename TGetCompletedHandler, typename TAdditionalContext>
            void HandleGetCompleteItemNotFound(TGetCompletedHandler getCompletedHandler,
                                               TAdditionalContext additionalContext)
            {
                m_pProxy->HandleGetCompleteItemNotFound(getCompletedHandler, additionalContext);
            }
            
        private:
            TProxiedGetCompleteHandlerObject* m_pProxy;
        };
        
        template<typename TProtocolTraits>
        class SimulatedStorageComponent : private boost::noncopyable
        {
            typedef typename TProtocolTraits::TStorageKey TKey;
            typedef typename TProtocolTraits::TEventSchedule TEventSchedule;
            typedef typename TProtocolTraits::TSerializedDataType TSerializedDataType;
            typedef typename TProtocolTraits::TDeserializedValueType TDeserializedValueType;
            typedef typename TProtocolTraits::TClientInputValueType TClientInputValueType;
            typedef typename TProtocolTraits::TReplicationMessageType TReplicationMessageType;
            
        public:
            typedef SimulatedStorageComponent<TProtocolTraits> TContext;
            
            SimulatedStorageComponent(const std::string& storageName,
                                      TEventSchedule& schedule,
                                      KeyValueStorage<TKey, TSerializedDataType>& storage)
            : m_maxConcurrentOperations(1000)
            , m_latency(450)
            , m_storageName(storageName)
            , m_schedule(schedule)
            , m_store(storage)
            {
                
            }
            
            template <typename TResultHandler, typename TRequestMetaData, typename TAdditionalContext=void*>
            void Get(const TKey& key,
                     TResultHandler& getHandler,
                     TRequestMetaData& requestMetaData,
                     TAdditionalContext additionalContext=nullptr)
            {
                typedef Components::GetCompleteHandlerProxy<TResultHandler> TResultHandlerProxy;
                typedef Components::GetCompleteHandlerProxy<TRequestMetaData> TRequestMetaDataProxy;
                typedef DeferredGetEvent<TResultHandlerProxy, TRequestMetaDataProxy, TAdditionalContext> TEvent;
                typedef std::shared_ptr<TEvent> TEventPtr;
                typedef SimRunner::SimulationInfrastructure::SimulationEvent TSimEvent;
                
                TEventPtr pEvent(new TEvent(*this,
                                            key,
                                            TResultHandlerProxy(getHandler),
                                            TRequestMetaDataProxy(requestMetaData),
                                            additionalContext));
                
                boost::function<void ()> bound = boost::bind(&TEvent::Handle, pEvent);
                TSimEvent event = m_schedule.BuildSimulationEventTimeStampedNow(bound, "Stable storage get");
                m_schedule.InsertSimulationEvent(event, m_latency);
            }
            
            template <typename TResultHandler>
            void Get(const TKey& key,
                     TResultHandler& getHandler,
                     TReplicationMessageType requestMetaData,
                     void* pDummy=nullptr)
            {
                typedef Components::GetCompleteHandlerProxy<TResultHandler> TResultHandlerProxy;
                typedef DeferredGetEvent<TResultHandlerProxy, TReplicationMessageType, void*> TEvent;
                typedef std::shared_ptr<TEvent> TEventPtr;
                typedef SimRunner::SimulationInfrastructure::SimulationEvent TSimEvent;
                
                TEventPtr pEvent(new TEvent(*this,
                                            key,
                                            TResultHandlerProxy(getHandler),
                                            requestMetaData,
                                            pDummy));
                
                boost::function<void ()> bound = boost::bind(&TEvent::Handle, pEvent);
                TSimEvent event = m_schedule.BuildSimulationEventTimeStampedNow(bound, "Stable storage get");
                m_schedule.InsertSimulationEvent(event, m_latency);
            }
            
            template <typename TResultHandler, typename TRequestMetaData>
            void Put(const TKey& key,
                     const TSerializedDataType& serializedValue,
                     TResultHandler& putHandler,
                     TRequestMetaData& requestMetaData,
                     const TDeserializedValueType& deserializedValue,
                     void* pDummy=nullptr)
            {
                typedef Components::PutCompleteHandlerProxy<TResultHandler> TResultHandlerProxy;
                typedef Components::PutCompleteHandlerProxy<TRequestMetaData> TRequestMetaDataProxy;
                typedef DeferredPutEvent<TResultHandlerProxy, TRequestMetaDataProxy> TEvent;
                typedef std::shared_ptr<TEvent> TEventPtr;
                typedef SimRunner::SimulationInfrastructure::SimulationEvent TSimEvent;
                
                TEventPtr pEvent(new TEvent(*this,
                                            key,
                                            serializedValue,
                                            TResultHandlerProxy(putHandler),
                                            TRequestMetaDataProxy(requestMetaData),
                                            deserializedValue));
                
                boost::function<void ()> bound = boost::bind(&TEvent::Handle, pEvent);
                
                TSimEvent event = m_schedule.BuildSimulationEventTimeStampedNow(bound, "Stable storage put");
                m_schedule.InsertSimulationEvent(event, m_latency);
            }
            
            
            template <typename TResultHandler>
            void Put(const TKey& key,
                     const TSerializedDataType& serializedValue,
                     TResultHandler& putHandler,
                     TReplicationMessageType requestMetaData,
                     const TDeserializedValueType& deserializedValue)
            {
                typedef Components::PutCompleteHandlerProxy<TResultHandler> TResultHandlerProxy;
                typedef DeferredPutEvent<TResultHandlerProxy, TReplicationMessageType> TEvent;
                typedef std::shared_ptr<TEvent> TEventPtr;
                typedef SimRunner::SimulationInfrastructure::SimulationEvent TSimEvent;
                
                TEventPtr pEvent(new TEvent(*this,
                                            key,
                                            serializedValue,
                                            TResultHandlerProxy(putHandler),
                                            requestMetaData,
                                            deserializedValue));
                
                boost::function<void ()> bound = boost::bind(&TEvent::Handle, pEvent);
                
                TSimEvent event = m_schedule.BuildSimulationEventTimeStampedNow(bound, "Stable storage put");
                m_schedule.InsertSimulationEvent(event, m_latency);
            }
            
            template <typename TResultHandler, typename TRequestMetaData, typename TAdditionalContext=void*>
            void PerformGet(const TKey& key, TResultHandler& getHandler, TRequestMetaData& requestMetaData, TAdditionalContext additionalContext)
            {
                TSerializedDataType* pValue;
                if(m_store.TryGetValue(key, pValue))
                {
                    //self.__metrics.add_persistent_store_get(self.__storage_name, key, True, result)
                    getHandler.HandleGetCompleteItemFound(requestMetaData, *pValue, additionalContext);
                }
                else
                {
                    //self.__metrics.add_persistent_store_get(self.__storage_name, key, False)
                    getHandler.HandleGetCompleteItemNotFound(requestMetaData, additionalContext);
                }
            }
            
            template <typename TResultHandler, typename TRequestMetaData>
            void PerformPut(const TKey& key,
                            const TSerializedDataType& value,
                            TResultHandler& putHandler,
                            TRequestMetaData& requestMetaData,
                            TDeserializedValueType& deserializedValueType)
            {
                //self.__metrics.add_persistent_store_put(self.__storage_name, key, value)
                m_store.AddValueForKey(key, value);
                putHandler.HandlePutComplete(requestMetaData, deserializedValueType);
            }
            
        private:
            const size_t m_maxConcurrentOperations;
            const SimulationInfrastructure::SimulationTimeMicroSeconds m_latency;
            
            std::string m_storageName;
            TEventSchedule& m_schedule;
            KeyValueStorage<TKey, TSerializedDataType>& m_store;
            
            template <typename TResultHandler, typename TRequestMetaData, typename TAdditionalContext>
            class DeferredGetEvent : private boost::noncopyable
            {
                TContext& m_component;
                TKey m_key;
                TResultHandler m_eventHandler;
                TRequestMetaData m_requestMetaData;
                TAdditionalContext m_context;
                
            public:
                DeferredGetEvent(TContext& component,
                                 const TKey& key,
                                 TResultHandler eventHandler,
                                 TRequestMetaData requestMetaData,
                                 TAdditionalContext context)
                : m_component(component)
                , m_key(key)
                , m_eventHandler(eventHandler)
                , m_requestMetaData(requestMetaData)
                , m_context(context)
                {
                }
                
                void Handle()
                {
                    m_component.PerformGet(m_key, m_eventHandler, m_requestMetaData, m_context);
                }
            };
            
            template <typename TResultHandler, typename TRequestMetaData>
            class DeferredPutEvent : private boost::noncopyable
            {
                TContext& m_component;
                TKey m_key;
                TSerializedDataType m_value;
                TResultHandler m_eventHandler;
                TRequestMetaData m_requestMetaData;
                TDeserializedValueType m_deserializedValueType;
            public:
                DeferredPutEvent(TContext& component,
                                 const TKey& key,
                                 const TSerializedDataType& value,
                                 TResultHandler eventHandler,
                                 TRequestMetaData requestMetaData,
                                 const TDeserializedValueType& deserializedValueType)
                : m_component(component)
                , m_key(key)
                , m_value(value)
                , m_eventHandler(eventHandler)
                , m_requestMetaData(requestMetaData)
                , m_deserializedValueType(deserializedValueType)
                {
                }
                
                void Handle()
                {
                    m_component.PerformPut(m_key, m_value, m_eventHandler, m_requestMetaData, m_deserializedValueType);
                }
            };
        };
    }
}
