//
//  OrbeDelayedInstallMessaging.h
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
        namespace OrbeDelayedInstall
        {
            namespace Messaging
            {
                template <typename ProtocolTraits>
                class OrbeDelayedInstallGetReply;
                
                template <typename ProtocolTraits>
                class OrbeDelayedInstallPutReply;
                
                template <typename ProtocolTraits>
                class OrbeDelayedInstallReplicateMessage;
            }
        }
    }
}
