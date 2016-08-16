//
//  OrbePartitionServerGetBeforePutContext.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
//
//

#pragma once

#include "Timing.h"
#include "Orbe.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            template<typename TProtocolTraits, typename TPutCompletedHandler, typename TContext>
            class OrbePartitionServerGetBeforePutContext
            {
                typedef typename TProtocolTraits::TStorageKey TStorageKey;
                typedef typename TProtocolTraits::TDependencyMatrix TDependencyMatrix;
                typedef typename TProtocolTraits::TClientInputValueType TClientInputValueType;
                
            public:
                OrbePartitionServerGetBeforePutContext(const TStorageKey& key,
                                                       const TDependencyMatrix& matrix,
                                                       const TClientInputValueType& value,
                                                       TPutCompletedHandler& putCompletedHandler,
                                                       const TContext& context)
                : m_key(key)
                , m_matrix(matrix)
                , m_value(value)
                , m_putCompletedHandler(putCompletedHandler)
                , m_context(context)
                {
                    
                }
                
                const TStorageKey& Key() const { return m_key; }
                
                const TDependencyMatrix& Matrix() const { return m_matrix; }
                
                const TClientInputValueType& Value() const { return m_value; }
                
                TPutCompletedHandler& PutCompletedHandler() const { return m_putCompletedHandler; }

                const TContext& Context() const { return m_context; }
                
            private:
                TStorageKey m_key;
                TDependencyMatrix m_matrix;
                TClientInputValueType m_value;
                TPutCompletedHandler& m_putCompletedHandler;
                TContext m_context;
            };
        }
    }
}
