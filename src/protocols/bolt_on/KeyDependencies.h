//
//  KeyDependencies.h
//  SimRunner
//
//  Created by Scott on 15/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <vector>
#include <set>
#include "BoltOn.h"
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            template<typename TProtocolTraits>
            class KeyDependencies
            {
                typedef typename TProtocolTraits::TValueWrapperPtr TValueWrapperPtr;
                typedef typename TProtocolTraits::TCausalClock TCausalClock;
                typedef typename TProtocolTraits::TStorageKey TStorageKey;
                typedef typename TProtocolTraits::TKeyDependenciesMapping TKeyDependenciesMapping;
                typedef typename TProtocolTraits::TKeyDependencies TKeyDependencies;
                
                KeyDependencies() { }
                
            public:
                static KeyDependencies CreateEmpty() { return KeyDependencies(); }
                
                KeyDependencies(const TKeyDependenciesMapping& keyDependenciesMapping)
                : m_keyDependenciesMapping(keyDependenciesMapping)
                {
                    
                }
                
                KeyDependencies(const std::set<TValueWrapperPtr>& valueWrappers)
                {
                    for(auto valueWrapperIt = valueWrappers.begin(); valueWrapperIt != valueWrappers.end(); ++ valueWrapperIt)
                    {
                        const TValueWrapperPtr valueWrapper = *valueWrapperIt;
                        const TKeyDependencies& dependencies(valueWrapper->KeyDependencies());
                        const std::vector<TStorageKey>& keys(dependencies.DependencyKeys());
                        
                        for(auto keyIt = keys.begin(); keyIt != keys.end(); ++ keyIt)
                        {
                            const TStorageKey& key(*keyIt);
                            const TCausalClock& clock(dependencies.GetClock(key));
                            
                            if(ContainsKey(key))
                            {
                                TCausalClock localClock(GetClock(key));
                                localClock.MergeWithClock(clock);
                                PutDependency(key, localClock);
                            }
                            else
                            {
                                PutDependency(key, clock);
                            }
                        }
                    }
                }
                
                const TKeyDependenciesMapping& Dependencies() const
                {
                    return m_keyDependenciesMapping;
                }
                
                std::vector<TStorageKey> DependencyKeys() const
                {
                    std::vector<TStorageKey> output;
                    
                    for(auto it = m_keyDependenciesMapping.begin(); it != m_keyDependenciesMapping.end(); ++ it)
                    {
                        output.push_back(it->first);
                    }
                    
                    return output;
                }
                
                bool ContainsKey(const TStorageKey& key) const
                {
                    return m_keyDependenciesMapping.find(key) != m_keyDependenciesMapping.end();
                }
                
                const TCausalClock& GetClock(const TStorageKey& key) const
                {
                    SR_ASSERT(ContainsKey(key));
                    return m_keyDependenciesMapping.find(key)->second;
                }
                
                TKeyDependencies& PutDependency(const TStorageKey& key, const TCausalClock& dependencyClock)
                {
                    auto findResult(m_keyDependenciesMapping.find(key));
                    
                    if(findResult != m_keyDependenciesMapping.end())
                    {
                        m_keyDependenciesMapping.erase(findResult);
                    }
                    
                    auto result = m_keyDependenciesMapping.insert(std::make_pair(key, dependencyClock));
                    SR_ASSERT(result.second);
                    
                    return *this;
                }
                
            private:
                TKeyDependenciesMapping m_keyDependenciesMapping;
            };
        }
    }
}
