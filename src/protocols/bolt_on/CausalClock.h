//
//  CausalClock.h
//  SimRunner
//
//  Created by Scott on 15/12/2014.
//

#pragma once

#include <vector>
#include <algorithm>
#include "BoltOn.h"
#include "CausalRelationship.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            template<typename TProtocolTraits>
            class CausalClock
            {
                typedef typename TProtocolTraits::TShimId TShimId;
                typedef typename TProtocolTraits::TShimIdToWritesMapping TShimIdToWritesMapping;
                typedef typename TProtocolTraits::TCounter TCounter;
                typedef typename TProtocolTraits::TCounterValue TCounterValue;
                typedef typename TProtocolTraits::TCausalClock TCausalClock;
                
                CausalClock() { }
                
            public:
                static CausalClock CreateEmpty() { return CausalClock(); }
                
                CausalClock(const TShimIdToWritesMapping& shimIdToWritesMapping)
                : m_shimIdToWritesMapping(shimIdToWritesMapping)
                {
                    
                }
                
                std::vector<TShimId> Writers() const
                {
                    std::vector<TShimId> output;
                    
                    for(auto it = m_shimIdToWritesMapping.begin(); it != m_shimIdToWritesMapping.end(); ++ it)
                    {
                        output.push_back(it->first);
                    }
                    
                    return output;
                }
                
                const TShimIdToWritesMapping& Data() const
                {
                    return m_shimIdToWritesMapping;
                }
                
                void SetValueForShim(const TShimId& shimId, const TCounterValue& value)
                {
                    if(HasWriter(shimId))
                    {
                        m_shimIdToWritesMapping.find(shimId)->second.SetValue(value);
                    }
                    else
                    {
                        m_shimIdToWritesMapping.insert(std::make_pair(shimId, TCounter(value)));
                    }
                }
                
                TCounterValue GetValueForShim(const TShimId& shimId) const
                {
                    if(HasWriter(shimId))
                    {
                        return m_shimIdToWritesMapping.find(shimId)->second.Value();
                    }
                    else
                    {
                        return TCounterValue();
                    }
                }
                
                void IncrementValueForShim(const TShimId& shimId)
                {
                    if(HasWriter(shimId))
                    {
                        m_shimIdToWritesMapping[shimId].IncrementAndGet();
                    }
                    else
                    {
                        m_shimIdToWritesMapping[shimId] = TCounter(1);
                    }
                }
                
                void MergeWithClock(const TCausalClock& clockToMerge)
                {
                    const std::vector<TShimId>& otherClockWriterShims(clockToMerge.Writers());
                    
                    for(auto it = otherClockWriterShims.begin(); it != otherClockWriterShims.end(); ++ it)
                    {
                        const TShimId& shimId(*it);
                        
                        const TCounterValue candidateValue(clockToMerge.GetValueForShim(shimId));
                        TCounterValue mergeResult(candidateValue);
                        
                        if(HasWriter(shimId))
                        {
                            const TCounterValue currentValue(GetValueForShim(shimId));
                            mergeResult = std::max(candidateValue, currentValue);
                        }
                        
                        SetValueForShim(shimId, mergeResult);
                    }
                }
                
                CausalRelationship Compare(const TCausalClock& otherClock) const
                {
                    const std::vector<TShimId> myWriters(Writers());
                    const std::vector<TShimId> otherClockWriters(otherClock.Writers());
                    
                    std::set<TShimId> allWriterIds;
                    for(auto it = myWriters.begin(); it != myWriters.end(); ++ it) { allWriterIds.insert(*it); }
                    for(auto it = otherClockWriters.begin(); it != otherClockWriters.end(); ++ it) { allWriterIds.insert(*it); }
                    
                    bool earlier = false;
                    bool later = false;
                    
                    for(auto it = allWriterIds.begin(); it != allWriterIds.end(); ++ it)
                    {
                        const TShimId& shimId(*it);
                        
                        const TCounterValue myValue(GetValueForShim(shimId));
                        const TCounterValue otherClockValue(otherClock.GetValueForShim(shimId));
                        
                        if(myValue < otherClockValue)
                        {
                            earlier = true;
                        }
                        else if(myValue > otherClockValue)
                        {
                            later = true;
                        }
                    }
                    
                    if(earlier && !later)
                    {
                        return CausalRelationship::HappensBefore;
                    }
                    else if(later && !earlier)
                    {
                        return CausalRelationship::HappensAfter;
                    }
                    else if(earlier && later)
                    {
                        return CausalRelationship::Concurrent;
                    }
                    else
                    {
                        return CausalRelationship::Equal;
                    }
                }
                
            private:
                bool HasWriter(const TShimId& shimId) const
                {
                    return m_shimIdToWritesMapping.find(shimId) != m_shimIdToWritesMapping.end();
                }
                
                TShimIdToWritesMapping m_shimIdToWritesMapping;
            };
        }
    }
}
