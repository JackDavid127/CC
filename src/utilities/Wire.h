//
//  Wire.h
//  SimRunner
//
//  Created by Scott on 21/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <cstdlib>
#include <vector>
#include <boost/noncopyable.hpp>
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace Utilities
    {
        typedef uint8_t Byte;
        typedef std::vector<Byte> ByteBuffer;
        
        inline Byte ToNetwork(Byte value) { return value; }
        inline Byte ToHost(Byte value) { return value; }
        
        int16_t ToNetwork(int16_t value);
        int32_t ToNetwork(int32_t value);
        int64_t ToNetwork(int64_t value);
        
        int16_t ToHost(int16_t value);
        int32_t ToHost(int32_t value);
        int64_t ToHost(int64_t value);
        
        uint16_t ToNetwork(uint16_t value);
        uint32_t ToNetwork(uint32_t value);
        uint64_t ToNetwork(uint64_t value);
        
        uint16_t ToHost(uint16_t value);
        uint32_t ToHost(uint32_t value);
        uint64_t ToHost(uint64_t value);
        
        class SubBuffer
        {
        public:
            SubBuffer(ByteBuffer& buffer, size_t cursor, size_t size)
            : m_buffer(buffer)
            , m_cursor(cursor)
            , m_size(size)
            {
                SR_ASSERT(cursor + size <= buffer.size(),
                          "Error in SubBuffer creation: cursor: %lu, sub-buffer size: %lu, backing buffer size: %lu", cursor, size, buffer.size());
            }
            
            const Byte& operator[] (size_t n) const
            {
                SR_ASSERT(n < m_size);
                SR_ASSERT(m_cursor + n < m_buffer.size());
                return m_buffer[m_cursor + n];
            }
            
            size_t size() const
            {
                return m_size;
            }
            
        private:
            ByteBuffer& m_buffer;
            size_t m_cursor;
            size_t m_size;
        };
        
        template<typename TValueType>
        size_t ToNetworkBufferPush(TValueType value, ByteBuffer& buffer, size_t cursor)
        {
            TValueType result(ToNetwork(value));
            std::memcpy(&buffer[cursor], &result, sizeof(value));
            return cursor + sizeof(value);
        }
        
        template<typename TValueType>
        size_t ToHostBufferPop(TValueType& value, const SubBuffer& buffer, size_t cursor)
        {
            TValueType result;
            std::memcpy(&result, &buffer[cursor], sizeof(result));
            value = ToHost(result);
            return cursor + sizeof(result);
        }
        
        template<size_t MessageHeaderBytes>
        class BufferReader : boost::noncopyable
        {
        public:
            BufferReader(const SubBuffer& buffer)
            : m_buffer(buffer)
            , m_cursor(MessageHeaderBytes)
            {
            }
            
            bool Done() const { return m_buffer.size() == m_cursor; }
            
            template<typename TValueType>
            TValueType Read()
            {
                SR_ASSERT(!Done());
                const size_t valueSize(sizeof(TValueType));
                SR_ASSERT((m_cursor + valueSize) <= m_buffer.size());
                TValueType value;
                m_cursor = Utilities::ToHostBufferPop(value, m_buffer, m_cursor);
                return value;
            }
            
            void ReadByteStream(char* destination, size_t length)
            {
                SR_ASSERT(!Done());
                SR_ASSERT((m_cursor + length) <= m_buffer.size());
                std::memcpy(destination, &m_buffer[m_cursor], length);
                m_cursor += length;
            }
            
        private:
            const SubBuffer& m_buffer;
            size_t m_cursor;
        };
        
        template<typename MessageHeaderType>
        class BufferWriter : boost::noncopyable
        {
            typedef BufferWriter<MessageHeaderType> TSelf;
            
        public:
            BufferWriter(ByteBuffer& buffer, MessageHeaderType bodyBytes)
            : m_buffer(buffer)
            , m_bodyBytes(bodyBytes)
            , m_cursor(0)
            {
                m_buffer.resize(sizeof(MessageHeaderType) + m_bodyBytes);
                Write<MessageHeaderType>(m_bodyBytes);
            }
            
            bool Done() const { return m_buffer.size() == m_cursor; }
            
            template<typename TValueType>
            TSelf& Write(TValueType value)
            {
                SR_ASSERT(!Done());
                const size_t valueSize(sizeof(TValueType));
                SR_ASSERT((m_cursor + valueSize) <= m_buffer.size());
                m_cursor = Utilities::ToNetworkBufferPush(value, m_buffer, m_cursor);
                return *this;
            }
            
            TSelf& WriteByteStream(const char* value, size_t length)
            {
                SR_ASSERT(!Done());
                SR_ASSERT((m_cursor + length) <= m_buffer.size());
                std::memcpy(&m_buffer[m_cursor], value, length);
                m_cursor += length;
                return *this;
            }
            
        private:
            MessageHeaderType m_bodyBytes;
            ByteBuffer& m_buffer;
            size_t m_cursor;
        };
        
        template <class TCharContainer>
        std::string DumpHex(const TCharContainer& buffer)
        {
            std::string output;
            
            for (const auto& byte : buffer)
            {
                char tempBuf[16];
                std::sprintf(tempBuf, "%02X ", byte);
                output += tempBuf;
            }
            
            return output;
        }
    }
}