//
//  ECMessaging.h
//  SimRunner
//
//  Created by Scott on 08/10/2014.
//
//

#pragma once

namespace SimRunner
{
    namespace Protocols
    {
        namespace EC
        {
            namespace Messaging
            {
                template <typename TClientInputValueType>
                class ECGetReply;
                
                class ECPutReply;
                
                template <typename TKey, typename TClientInputValueType, typename TDeserializedValueType>
                class ECReplicateMessage;
            }
        }
    }
}
