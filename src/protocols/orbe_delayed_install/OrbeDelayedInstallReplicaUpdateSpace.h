//
//  OrbeDelayedInstallReplicaUpdateSpace.h
//  SimRunner
//
//  Created by Scott on 04/01/2015.
//  Copyright (c) 2015 Scott. All rights reserved.
//

#pragma once

#include <boost/noncopyable.hpp>
#include <vector>
#include "KeyValueStorage.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            template <typename TProtocolTraits>
            class OrbeDelayedInstallReplicaUpdateSpace : private boost::noncopyable
            {
                typedef typename TProtocolTraits::TStorageKey TKey;
                typedef typename TProtocolTraits::TDeserializedValueType TDeserializedValueType;
                typedef typename TProtocolTraits::TVersionVector TVersionVector;
                
            public:
                
                void Put(const TKey& key, const TDeserializedValueType& deserializedValueType)
                {
                    std::vector<TDeserializedValueType>* pKeyValues;
                    
                    if(!m_data.TryGetValue(key, pKeyValues))
                    {
                        m_data.Put(key, std::vector<TDeserializedValueType>());
                        const bool gotData(m_data.TryGetValue(key, pKeyValues));
                        SR_ASSERT(gotData);
                    }
                    
                    pKeyValues->push_back(deserializedValueType);
                }
                
                void GetSafeUpdates(const TVersionVector& versionVector, std::vector<TDeserializedValueType>& out_updates)
                {
                    for(auto it = m_data.begin(); it != m_data.end();)
                    {
                        //auto& key(it->first);
                        auto& values(it->second);
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
                            m_data.erase(it++);
                        }
                        else
                        {
                            ++ it;
                        }
                    }
                }
                
            private:
                Components::KeyValueStorage<TKey, std::vector<TDeserializedValueType> > m_data;
            };
        }
    }
}