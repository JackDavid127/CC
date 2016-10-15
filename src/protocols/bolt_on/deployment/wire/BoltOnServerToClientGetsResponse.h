//
//  BoltOnServerToClientGetsResponse.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "BoltOn.h"
#include "BoltOnWire.h"
#include "Wire.h"
#include "StringValueParser.h"
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
                    class BoltOnServerToClientGetsResponse
                    {
                        typedef BoltOnServerToClientGetsResponse TSelf;
                        typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                        typedef typename std::vector<TClientInputValueType> TValues;

                    public:
                        static TSelf CreateFound(TValues values)
                        {
                            return TSelf(Utilities::Byte(1), values);
                        }

                        static TSelf CreateNotFound()
                        {
                            TValues ans;
                            return TSelf(Utilities::Byte(0), ans);
                        }

                        BoltOnWireMessages MessageType() const { return m_messageType; }

                        Utilities::Byte FoundValue() const { return m_foundValue; }

                        TValues Values() const { return m_values; }

                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<BoltOnMessagePreamble> reader(buffer);
                            Utilities::Byte foundValue = reader.Read<Utilities::Byte>() != 0;
                            int size = reader.Read<int>();
                            TValues val;
                            while (size--){
                                TClientInputValueType value(ReadString(reader));
                                val.push_back(value);
                            }
                            return TSelf(foundValue, val);
                        }

                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            typedef Utilities::BufferWriter<BoltOnMessageHeaderType> TBufferWriter;

                            BoltOnMessageHeaderType BodySize = sizeof(m_messageType)
                            + sizeof(m_foundValue) + sizeof(int);
                            for (auto it = m_values.begin(); it != m_values.end(); it++){
                                BodySize += static_cast<BoltOnMessageHeaderType>(Utilities::ComputeStringSerializationSize(*it));
                            }

                            TBufferWriter writer(buffer, BodySize);

                            writer
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<Utilities::Byte>(m_foundValue)
                            .template Write<int>(m_values.size());

                            for (auto it = m_values.begin(); it != m_values.end(); it++){
                                WriteString(*it, writer);
                            }
                        }

                    private:
                        BoltOnServerToClientGetsResponse(Utilities::Byte foundValue, TValues values)
                        : m_messageType(Wire::ServerToClientGetsResponseMsg)
                        , m_foundValue(foundValue)
                        , m_values(values)
                        {

                        }

                        BoltOnWireMessages m_messageType;
                        Utilities::Byte m_foundValue;
                        TValues m_values;
                    };
                }
            }
        }
    }
}
