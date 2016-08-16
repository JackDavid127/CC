//
//  OrbeDelayedInstallWire.h
//  SimRunner
//
//  Created by Scott on 20/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "Wire.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            namespace Deployment
            {
                namespace Wire
                {
                    typedef uint32_t OrbeDelayedInstallMessageHeaderType;
                    constexpr size_t OrbeDelayedInstallMessageHeaderBytes = sizeof(uint32_t);
                    
                    static_assert(OrbeDelayedInstallMessageHeaderBytes == 4, "Expected a OrbeDelayedInstallMessageHeaderType size of 4 bytes, is this platform weird?");
                    
                    enum OrbeDelayedInstallWireMessages : Utilities::Byte
                    {
                        ClientToServerGetRequestMsg,
                        ClientToServerPutRequestMsg,
                        
                        ServerToServerGetRequestMsg,
                        ServerToServerPutRequestMsg,
                        
                        ServerToServerGetResponseMsg,
                        ServerToServerPutResponseMsg,
                        
                        ServerToClientGetResponseMsg,
                        ServerToClientPutResponseMsg,
                        
                        DependencyTimestampCheckRequestMsg,
                        DependencyTimestampCheckResponseMsg,
                        
                        ReplicateMsg,
                        ReplicaHeartbeatMsg,
                        
                        PartitionIdentityExchangeMsg,
                        ReplicaIdentityExchangeMsg,
                        
                        ManualClientHeartbeatMsg,
                        ManualClientStatusMsg
                    };
                    
                    constexpr size_t OrbeDelayedInstallMessagePreamble = sizeof(OrbeDelayedInstallWireMessages);
                    
                    typedef Utilities::BufferReader<OrbeDelayedInstallMessagePreamble> TBufferReader;
                }
            }
        }
    }
}
