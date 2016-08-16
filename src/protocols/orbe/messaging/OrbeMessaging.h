//
//  OrbeMessaging.h
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
        namespace Orbe
        {
            namespace Messaging
            {
                template <typename ProtocolTraits>
                class OrbeGetReply;
                
                template <typename ProtocolTraits>
                class OrbePutReply;
                
                template <typename ProtocolTraits>
                class OrbeReplicateMessage;
            }
        }
    }
}
