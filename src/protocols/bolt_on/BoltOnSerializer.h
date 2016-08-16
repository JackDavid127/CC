//
//  BoltOnSerializer.h
//  SimRunner
//
//  Created by Scott on 17/12/2014.
//

#pragma once

#include "BoltOn.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            template<typename TProtocolTraits>
            class BoltOnSerializer
            {
                typedef typename TProtocolTraits::TBoltOnSerializedValueTypePtr TBoltOnSerializedValueTypePtr;
                typedef typename TProtocolTraits::TValueWrapperFactory TValueWrapperFactory;
                typedef typename TProtocolTraits::TValueWrapperPtr TValueWrapperPtr;
                
            public:
                BoltOnSerializer(TValueWrapperFactory& valueWrapperFactory)
                : m_valueWrapperFactory(valueWrapperFactory)
                {
                    
                }
                
                TValueWrapperPtr Deserialize(const TBoltOnSerializedValueTypePtr serialized) const
                {
                    return m_valueWrapperFactory.CreateValueWrapper(serialized);
                }
                
            private:
                TValueWrapperFactory& m_valueWrapperFactory;
            };
        }
    }
}
