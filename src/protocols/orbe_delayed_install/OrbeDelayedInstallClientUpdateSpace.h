//
//  OrbeDelayedInstallClientUpdateSpace.h
//  SimRunner
//
//  Created by Scott on 03/01/2015.
//  Copyright (c) 2015 Scott. All rights reserved.
//

#pragma once

#include <boost/noncopyable.hpp>
#include <vector>
#include "KeyValueStorage.h"
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            template <typename TProtocolTraits>
            class OrbeDelayedInstallClientUpdateSpace : private boost::noncopyable
            {
                typedef typename TProtocolTraits::TStorageKey TKey;
                typedef typename TProtocolTraits::TDeserializedValueType TDeserializedValueType;
                typedef typename TProtocolTraits::TVersionVector TVersionVector;
                typedef typename TProtocolTraits::TClientId TClientId;
                
            public:
                
                bool TryGet(const TClientId& clientId, const TKey& key, TDeserializedValueType*& out_pValue)
                {
                    Components::KeyValueStorage<TKey, std::vector<TDeserializedValueType> >* pClientValues;
                    
                    if(m_data.TryGetValue(clientId, pClientValues))
                    {
                        std::vector<TDeserializedValueType>* pKeyValues;
                        
                        if(pClientValues->TryGetValue(key, pKeyValues))
                        {
                            out_pValue = &pKeyValues->back();
                            return true;
                        }
                        else
                        {
                            out_pValue = nullptr;
                            return false;
                        }
                    }
                    else
                    {
                        out_pValue = nullptr;
                        return false;
                    }
                }
                
                void Put(const TClientId& clientId, const TKey& key, const TDeserializedValueType& deserializedValueType)
                {
                    Components::KeyValueStorage<TKey, std::vector<TDeserializedValueType> >* pClientValues;
                    
                    if(!m_data.TryGetValue(clientId, pClientValues))
                    {
                        Components::KeyValueStorage<TKey, std::vector<TDeserializedValueType> > value;
                        m_data.Put(clientId, value);
                        const bool gotData(m_data.TryGetValue(clientId, pClientValues));
                        SR_ASSERT(gotData);
                    }
                    
                    std::vector<TDeserializedValueType>* pKeyValues;
                    
                    if(!pClientValues->TryGetValue(key, pKeyValues))
                    {
                        pClientValues->Put(key, std::vector<TDeserializedValueType>());
                        const bool gotData(pClientValues->TryGetValue(key, pKeyValues));
                        SR_ASSERT(gotData);
                    }
                    
                    pKeyValues->push_back(deserializedValueType);
                }
                
                void GetSafeUpdates(const TVersionVector& versionVector, std::vector<TDeserializedValueType>& out_updates)
                {
                    for(auto outerIt = m_data.begin(); outerIt != m_data.end();)
                    {
                        //auto& client(it->first);
                        auto& clientData(outerIt->second);
                        
                        for(auto innerIt = clientData.begin(); innerIt != clientData.end();)
                        {
                            //auto& key(it->first);
                            auto& values(innerIt->second);
                            const auto valueCount(values.size());
                            
                            for(auto i = 0; i < valueCount; ++ i)
                            {
                                const auto valueIndex = (valueCount - 1) - i;
                                auto& value(values[valueIndex]);
                                
                                if(value.LogicalTimestamp() <= versionVector.GetValueForReplica(value.SourceReplicaId()))
                                {
                                    out_updates.push_back(value);
                                    std::swap(value, values.back());
                                    values.pop_back();
                                }
                            }
                            
                            if(values.empty())
                            {
                                //printf("key delete: %d, for client %d\n", innerIt->first, outerIt->first);
                                //innerIt = clientData.erase(innerIt);
                                clientData.erase(innerIt ++);
                            }
                            else
                            {
                                ++ innerIt;
                            }
                        }
                        
                        if(clientData.empty())
                        {
                            //printf("client delete: %d\n", outerIt->first);
                            //outerIt = m_data.erase(outerIt);
                            m_data.erase(outerIt ++);
                        }
                        else
                        {
                            ++ outerIt;
                        }
                    }
                }
                
            private:
                Components::KeyValueStorage<TClientId, Components::KeyValueStorage<TKey, std::vector<TDeserializedValueType> > > m_data;
            };
        }
    }
}