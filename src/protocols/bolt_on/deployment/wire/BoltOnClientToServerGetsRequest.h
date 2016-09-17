//
//  BoltOnClientToServerGetRequest.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "BoltOn.h"
#include "BoltOnWire.h"
#include "Wire.h"
#include "Keys.h"
#include <vector>

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            namespace Deployment
            {
                namespace Wire
                {
                    template <typename ProtocolTraits>
                    class BoltOnClientToServerGetsRequest
                    {
                        typedef BoltOnClientToServerGetsRequest TSelf;
                        typedef Keys::KeyType TKeyType;
                        typedef typename std::vector<TKeyType> TKeys;

                    public:
                        static TSelf Create(TKeys keys)
                        {
                            return TSelf(keys);
                        }

                        BoltOnWireMessages MessageType() const { return m_messageType; }

                        TKeys Keys() const { return m_keys; }

                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<BoltOnMessagePreamble> reader(buffer);
                            int sum = reader.Read<int>();
                            TKeys keys;
                            while (sum--){
                                PartitionIdType key = reader.Read<TKeyType>();
                                keys.push_back(key);
                            }
                            return TSelf(keys);
                        }

                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            BoltOnMessageHeaderType BodySize = m_keys.size() * sizeof(TKeyType) + sizeof(m_messageType);
                            Utilities::BufferWriter<BoltOnMessageHeaderType> itt(buffer, BodySize);
                            itt.template Write<Utilities::Byte>(m_messageType);
                            itt.template Write<int>(m_keys.size());
                            for (auto it = m_keys.begin(); it != m_keys.end(); it++){
                                itt.template Write<TKeyType>(*it);
                            }
                        }

                    private:
                        BoltOnClientToServerGetsRequest(TKeys keys)
                        : m_messageType(Wire::ClientToServerGetsRequestMsg)
                        , m_keys(keys)
                        {

                        }

                        BoltOnWireMessages m_messageType;
                        TKeys m_keys;

                        //static constexpr BoltOnMessageHeaderType BodySize = sizeof(m_messageType)
                        //+ sizeof(m_key);
                    };
                }
            }
        }
    }
}
