//
//  BoltOnSimulatedBackingStorage.h
//  SimRunner
//
//  Created by Scott on 17/12/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include "KeyValueStorage.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            namespace SimulatedComponents
            {
                template<typename TProtocolTraits>
                class BoltOnSimulatedBackingStorage : private boost::noncopyable
                {
                    typedef typename TProtocolTraits::TStorageKey TStorageKey;
                    typedef typename TProtocolTraits::TBoltOnSerializedValueTypePtr TBoltOnSerializedValueTypePtr;
                    
                public:
                    BoltOnSimulatedBackingStorage(Components::KeyValueStorage<TStorageKey, TBoltOnSerializedValueTypePtr>& storage)
                    : m_storage(storage)
                    {
                        
                    }
                    
                    std::unique_ptr<BoltOnSimulatedBackingStorage> Clone()
                    {
                        return std::unique_ptr<BoltOnSimulatedBackingStorage>(new BoltOnSimulatedBackingStorage(m_storage));
                    }
                    
                    void Get(const TStorageKey& key,
                             boost::function<void (const TBoltOnSerializedValueTypePtr)> getCompletionCallback)
                    {
                        TBoltOnSerializedValueTypePtr* pResult;
                        
                        if(m_storage.TryGetValue(key, pResult))
                        {
                            getCompletionCallback(*pResult);
                        }
                        else
                        {
                            getCompletionCallback(nullptr);
                        }
                    }
                    
                    void Put(const TStorageKey& key,
                             const TBoltOnSerializedValueTypePtr serializedData,
                             const boost::function<void ()>& putCompleteHandler)
                    {
                        m_storage.Put(key, serializedData);
                        putCompleteHandler();
                    }
                    
                private:
                    Components::KeyValueStorage<TStorageKey, TBoltOnSerializedValueTypePtr>& m_storage;
                };
            }
        }
    }
}
