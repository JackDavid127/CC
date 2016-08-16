//
//  OrbeServerStorage.h
//  SimRunner
//
//  Created by Scott on 20/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <memory>
#include <boost/noncopyable.hpp>
#include "SimulationInfrastructure.h"
#include "KeyValueStorage.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            namespace Deployment
            {
                namespace Server
                {
                    namespace Devices
                    {
                        template<
                        typename TKey,
                        typename TSerializedValueType,
                        typename TProtocolTraits>
                        class OrbeServerStorage : private boost::noncopyable
                        {
                        public:
                            
                            OrbeServerStorage(Components::KeyValueStorage<TKey, TSerializedValueType>& storage)
                            : m_store(storage)
                            {
                                
                            }
                            
                            template <typename TResultHandler, typename TRequestMetaData, typename TContext>
                            void Get(const TKey& key,
                                     TResultHandler& getHandler,
                                     TRequestMetaData& requestMetaData,
                                     TContext context)
                            {
                                TSerializedValueType* pValue;
                                
                                if(m_store.TryGetValue(key, pValue))
                                {
                                    getHandler.HandleGetCompleteItemFound(requestMetaData, *pValue, context);
                                }
                                else
                                {
                                    getHandler.HandleGetCompleteItemNotFound(requestMetaData, context);
                                }
                            }
                            
                            
                            template <typename TResultHandler, typename TRequestMetaData>
                            void Get(const TKey& key,
                                     TResultHandler& getHandler,
                                     TRequestMetaData& requestMetaData)
                            {
                                TSerializedValueType* pValue;
                                
                                if(m_store.TryGetValue(key, pValue))
                                {
                                    getHandler.HandleGetCompleteItemFound(requestMetaData, *pValue);
                                }
                                else
                                {
                                    getHandler.HandleGetCompleteItemNotFound(requestMetaData);
                                }
                            }
                            
                            template <typename TResultHandler, typename TRequestMetaData, typename TDeserializedValueType, typename TContext>
                            void Put(const TKey& key,
                                     const TSerializedValueType& serializedValue,
                                     TResultHandler& putHandler,
                                     TRequestMetaData& requestMetaData,
                                     const TDeserializedValueType& deserializedValue,
                                     TContext context)
                            {
                                m_store.AddValueForKey(key, serializedValue);
                                putHandler.HandlePutComplete(requestMetaData, deserializedValue, context);
                            }
                            
                            
                            template <typename TResultHandler, typename TRequestMetaData, typename TDeserializedValueType>
                            void Put(const TKey& key,
                                     const TSerializedValueType& serializedValue,
                                     TResultHandler& putHandler,
                                     TRequestMetaData& requestMetaData,
                                     const TDeserializedValueType& deserializedValue)
                            {
                                m_store.AddValueForKey(key, serializedValue);
                                putHandler.HandlePutComplete(requestMetaData, deserializedValue);
                            }
                            
                        private:
                            Components::KeyValueStorage<TKey, TSerializedValueType>& m_store;
                        };
                    }
                }
            }
        }
    }
}
