//
//  KeyValueStorage.h
//  SimRunner
//
//  Created by Scott on 12/10/2014.
//
//

#pragma once

#include <map>
#include <boost/noncopyable.hpp>
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace Components
    {
        template <typename TKey, typename TValue>
        class KeyValueStorage : private boost::noncopyable
        {
            typedef std::map<TKey, TValue> TStorage;
            typedef typename TStorage::iterator TStorageIterator;
            
        public:
            KeyValueStorage() { }
            
            KeyValueStorage(const KeyValueStorage& other)
            {
                m_storage = other.m_storage;
            }
            
            bool TryGetValue(const TKey& key, TValue*& out_pValue)
            {
                auto it = m_storage.find(key);
                
                if(it == m_storage.end())
                {
                    return false;
                }
                else
                {
                    out_pValue = &(it->second);
                    return true;
                }
            }
            
            void AddValueForKey(const TKey& key, const TValue& value)
            {
                auto findResult(m_storage.find(key));
                
                if(findResult != m_storage.end())
                {
                    m_storage.erase(findResult);
                }
                
                auto insertResult(m_storage.insert(std::make_pair(key, value)));
                
                // ensure insertion success
                SR_ASSERT(insertResult.second);
            }
            
            void Put(const TKey& key, const TValue& value)
            {
                AddValueForKey(key, value);
            }
            
            TStorageIterator begin() { return m_storage.begin(); }
            
            TStorageIterator end() { return m_storage.end(); }
            
            TStorageIterator erase(const TStorageIterator& it) { return m_storage.erase(it); }
            
            bool empty() const { return m_storage.empty(); }
            
        private:
            TStorage m_storage;
        };
    }
}
