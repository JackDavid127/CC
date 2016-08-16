
//
//  Connection.h
//  SimRunner
//
//  Created by Scott on 21/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <cstdlib>
#include <iostream>
#include <limits>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/circular_buffer.hpp>
#include "Wire.h"

//#define SR_DEBUG_CONNECTION_THREAD
//#define SR_DEBUG_CONNECTION_MESSAGESTAMP
//#define SR_DEBUG_CONNECTION_LOGGING

#if defined (SR_DEBUG_CONNECTION_LOGGING)
#define SR_DEBUG_CONNECTION_LOG(...) { if(m_log) printf(__VA_ARGS__); }
#else
#define SR_DEBUG_CONNECTION_LOG(x, ...) ((void)(x))
#endif

#if defined (SR_DEBUG_CONNECTION_THREAD)
#include <pthread.h>
#endif

namespace SimRunner
{
    namespace Utilities
    {
        void PrintCurrentThreadId()
        {
#if defined(SR_DEBUG_CONNECTION_THREAD)
            pthread_t ptid = pthread_self();
            uint64_t threadId = 0;
            memcpy(&threadId, &ptid, std::min(sizeof(threadId), sizeof(ptid)));
            SR_DEBUG_CONNECTION_LOG("Thread Id: %llu\n", threadId);
#endif
        }
        
        template<
        typename ProtocolTraits,
        typename TSelf,
        size_t MessageHeaderBytes>
        class Connection
        : public boost::noncopyable
        , public std::enable_shared_from_this<TSelf>
        {
            typedef TSelf TMessageProcessor;
            typedef uint32_t TMessageSizeBytesHeader;
            static constexpr size_t SizeOfMessageHeader = sizeof(TMessageSizeBytesHeader);
            
        protected:
            typedef typename ProtocolTraits::TLogger TLogger;
            
        public:
            typedef std::shared_ptr<TSelf> TPtr;
            
            boost::asio::ip::tcp::socket& Socket()
            {
                return m_socket;
            }
            
            std::string ConnectedHostAddress() const
            {
                boost::asio::ip::tcp::endpoint remote_ep = m_socket.remote_endpoint();
                boost::asio::ip::address remote_ad = remote_ep.address();
                return remote_ad.to_string();
            }
            
            void Start()
            {
                PrintCurrentThreadId();
                
                m_logger.Log("Connected to %s\n", ConnectedHostAddress().c_str());
                
                SR_DEBUG_CONNECTION_LOG("Connected to %s on %p\n", ConnectedHostAddress().c_str(), this);
                
                StartReadHeader();
                m_messageProcessor.HandleConnectionEstablished();
            }
            
        protected:
            Connection(boost::asio::io_service& ioService,
                       TLogger& logger,
                       TMessageProcessor& messageProcessor)
            : m_pSelf(nullptr)
            , m_ioService(ioService)
            , m_socket(ioService)
            , m_logger(logger)
            , m_messageProcessor(messageProcessor)
            , m_writeQueue(CircularBufferMaxCapacity)
            , m_pendingWriteBytes(0)
            , m_writing(false)
            
#if defined(SR_DEBUG_CONNECTION_LOGGING)
            , m_log(false)
#endif
            
#if defined (SR_DEBUG_CONNECTION_MESSAGESTAMP)
            , m_messageIndex(0)
#endif
            {
            }
            
            void SetupSharedPtrToSelf()
            {
                SR_ASSERT(m_pSelf == nullptr);
                m_pSelf = std::enable_shared_from_this<TSelf>::shared_from_this();
            }
            
            void EnableLogging()
            {
#if defined(SR_DEBUG_CONNECTION_LOGGING)
                m_log = true;
#endif
            }
            
            TPtr SharedPtrToSelf() const
            {
                SR_ASSERT(m_pSelf != nullptr);
                return m_pSelf;
            }
            
            template<typename TMessage>
            void ParseAndHandle(const Utilities::SubBuffer& subBuffer)
            {
                PrintCurrentThreadId();
                
                TMessage msg = TMessage::FromByteBuffer(subBuffer);
                m_messageProcessor.Handle(msg);
            }
            
            template<typename TMessage>
            void PostWrite(const TMessage& message)
            {
                PrintCurrentThreadId();
                
                m_ioService.post(boost::bind(&Connection::HandleWritePosted<TMessage>,
                                             SharedPtrToSelf(),
                                             message));
            }
            
        private:
            void StartReadHeader()
            {
                PrintCurrentThreadId();
                
                m_readDataBuffer.resize(SizeOfMessageHeader);
                
                boost::asio::async_read(m_socket,
                                        boost::asio::buffer(m_readDataBuffer),
                                        boost::bind(&Connection::HandleReadHeader,
                                                    SharedPtrToSelf(),
                                                    boost::asio::placeholders::error));
            }
            
            void HandleReadHeader(const boost::system::error_code& error)
            {
                PrintCurrentThreadId();
                
                if (!error)
                {
                    TMessageSizeBytesHeader totalSizeNetwork;
                    std::memcpy(&totalSizeNetwork, &m_readDataBuffer[0], sizeof(totalSizeNetwork));
                    const TMessageSizeBytesHeader totalSizeHost = ToHost(totalSizeNetwork);
                    StartReadBody(totalSizeHost);
                }
                else
                {
                    printf("ERROR! %s\n", error.message().c_str());
                }
            }
            
            void StartReadBody(TMessageSizeBytesHeader length)
            {
                PrintCurrentThreadId();
                
                SR_DEBUG_CONNECTION_LOG("StartReadBody length: %d\n", length);
                m_readDataBuffer.resize(SizeOfMessageHeader + length);
                boost::asio::mutable_buffers_1 subBuffer = boost::asio::buffer(&m_readDataBuffer[SizeOfMessageHeader], length);
                
                boost::asio::async_read(m_socket,
                                        subBuffer,
                                        boost::bind(&Connection::HandleReadBody,
                                                    SharedPtrToSelf(),
                                                    boost::asio::placeholders::error));
            }
            
            void HandleReadBody(const boost::system::error_code& error)
            {
                PrintCurrentThreadId();
                
                if(!error)
                {
                    ProcessBufferedMessages();
                    StartReadHeader();
                }
                else
                {
                    printf("ERROR! %s\n", error.message().c_str());
                }
            }
            
            void ProcessBufferedMessages()
            {
                size_t cursor(SizeOfMessageHeader);
                const size_t totalData(m_readDataBuffer.size());
                
                SR_DEBUG_CONNECTION_LOG("ProcessBufferedMessages: totalData: %lu\n", totalData);
                
                while(cursor != totalData)
                {
#if defined(SR_DEBUG_CONNECTION_MESSAGESTAMP)
                    uint32_t messageIndexNetwork;
                    std::memcpy(&messageIndexNetwork, &m_readDataBuffer[cursor], sizeof(messageIndexNetwork));
                    uint32_t messageIndexHost(Utilities::ToHost(messageIndexNetwork));
                    cursor += sizeof(uint32_t);
#endif
                    
                    const int16_t messageSize(GetSingleMessageSize(cursor));
                    
#if defined(SR_DEBUG_CONNECTION_MESSAGESTAMP)
                    SR_DEBUG_CONNECTION_LOG("Reading message %d, size: %hu, cursor: %lu, totalData: %lu\n", messageIndexHost, messageSize, cursor, totalData);
#endif
                    
                    SR_DEBUG_CONNECTION_LOG("ProcessBufferedMessages: cursor: %lu, messageSize: %hd\n", cursor, messageSize);
                    
                    SR_ASSERT((cursor + messageSize) <= totalData);
                    
                    Utilities::SubBuffer subBuffer(m_readDataBuffer, cursor, messageSize);
                    m_messageProcessor.ProcessBufferedMessage(subBuffer);
                    cursor += messageSize;
                    
                    SR_ASSERT(cursor <= totalData);
                }
                
                m_readDataBuffer.clear();
            }
            
            int16_t GetSingleMessageSize(size_t& inOut_Cursor)
            {
                size_t size(0);
                
                for (size_t i = 0; i < MessageHeaderBytes; ++i)
                {
                    size = (size << 8) + (static_cast<uint32_t>(m_readDataBuffer[inOut_Cursor + i]) & 0xFF);
                }
                
                inOut_Cursor += MessageHeaderBytes;
                
                return size;
            }
            
            template<typename TMessage>
            void HandleWritePosted(const TMessage message)
            {
                PrintCurrentThreadId();
                
                Utilities::ByteBuffer buffer;
                message.ToBuffer(buffer);
                
                //if(m_log) printf("HandleWritePosted %p:: write q size pre-push: %lu\n", this, m_writeQueue.size());
                m_writeQueue.push_back(buffer); // must push to buffer for storage for duration of async_write
                m_pendingWriteBytes += buffer.size();
                //if(m_log) printf("HandleWritePosted %p:: write q size post-push: %lu\n", this, m_writeQueue.size());
                
                if(!m_writing)
                {
                    // can only write if write not in progress
                    PerformWrite();
                }
            }
            
            void PerformWrite()
            {
                PrintCurrentThreadId();
                
                SR_ASSERT(!m_writing);
                SR_ASSERT(!m_writeQueue.empty());
                
                SR_DEBUG_CONNECTION_LOG("PerformWrite from %p:: write q size pre-async_write: %lu messages, %lu bytes\n",
                                        this,
                                        m_writeQueue.size(),
                                        m_pendingWriteBytes);
                
                m_writing = true;
                
#if defined(SR_DEBUG_CONNECTION_MESSAGESTAMP)
                const TMessageSizeBytesHeader totalMessagesSize(static_cast<TMessageSizeBytesHeader>(m_pendingWriteBytes + sizeof(uint32_t)*m_writeQueue.size()));
#else
                const TMessageSizeBytesHeader totalMessagesSize(static_cast<TMessageSizeBytesHeader>(m_pendingWriteBytes));
#endif
                m_concatenatedMessages.clear();
                m_concatenatedMessages.resize(SizeOfMessageHeader + totalMessagesSize);
                size_t cursor(0);
                
                TMessageSizeBytesHeader totalMessagesSizeNetwork(Utilities::ToNetwork(totalMessagesSize));
                std::memcpy(&m_concatenatedMessages[cursor], &totalMessagesSizeNetwork, sizeof(totalMessagesSizeNetwork));
                cursor += sizeof(totalMessagesSizeNetwork);
                
                while(!m_writeQueue.empty())
                {
                    Utilities::ByteBuffer buffer = m_writeQueue.front();
                    
#if defined(SR_DEBUG_CONNECTION_MESSAGESTAMP)
                    m_messageIndex++;
                    uint32_t messageIndexNetwork(Utilities::ToNetwork(m_messageIndex));
                    std::memcpy(&m_concatenatedMessages[cursor], &messageIndexNetwork, sizeof(messageIndexNetwork));
                    if(m_log) printf("Writing message %d, size: %lu, cursor: %lu\n", m_messageIndex, buffer.size(), cursor);
                    cursor += sizeof(uint32_t);
#endif
                    
                    std::memcpy(&m_concatenatedMessages[cursor], &buffer[0], buffer.size());
                    cursor += buffer.size();
                    m_writeQueue.pop_front();
                }
                
                SR_ASSERT(cursor == m_concatenatedMessages.size(), "cursor != concatenatedMessages.size(), %d, %lu\n", cursor, m_concatenatedMessages.size());
                SR_ASSERT(m_writeQueue.empty());
                
                m_pendingWriteBytes = 0;
                
                SR_DEBUG_CONNECTION_LOG("Writing %lu concatenatedMessages bytes.\n", m_concatenatedMessages.size());
                
                boost::asio::async_write(m_socket,
                                         boost::asio::buffer(m_concatenatedMessages),
                                         boost::bind(&Connection::HandleWriteFinished,
                                                     SharedPtrToSelf(),
                                                     boost::asio::placeholders::error));
                
                SR_DEBUG_CONNECTION_LOG("PerformWrite %p:: write q size post-async_write: %lu\n", this, m_writeQueue.size());
                
                //m_logger.Log("sent msg hex: %s\n", Utilities::DumpHex(m_concatenatedMessages).c_str());
            }
            
            void HandleWriteFinished(const boost::system::error_code& error)
            {
                PrintCurrentThreadId();
                
                SR_ASSERT(m_writing);
                
                m_logger.Log("write completed!\n");
                m_writing = false;
                
                if (!error)
                {
                    if(m_writeQueue.size() > 0)
                    {
                        PerformWrite();
                    }
                }
                else
                {
                    printf("ERROR! %s\n", error.message().c_str());
                }
            }
            
            static constexpr size_t CircularBufferMaxCapacity = std::numeric_limits<int64_t>::max();
            
            TPtr m_pSelf;
            
            boost::asio::io_service& m_ioService;
            boost::asio::ip::tcp::socket m_socket;
            TLogger& m_logger;
            TMessageProcessor& m_messageProcessor;
            
            Utilities::ByteBuffer m_readDataBuffer; //Storage for the *current* receive stream.
            boost::circular_buffer_space_optimized<Utilities::ByteBuffer> m_writeQueue; //Canonical storage for buffered messages before writing.
            size_t m_pendingWriteBytes; //Total number bytes in m_writeQueue.
            std::vector<Byte> m_concatenatedMessages; //Temporary storage for concatenated write buffers for merged write.
            bool m_writing;
            
#if defined(SR_DEBUG_CONNECTION_LOGGING)
            bool m_log;
#endif
            
#if defined(SR_DEBUG_CONNECTION_MESSAGESTAMP)
            uint32_t m_messageIndex;
#endif
        };
    }
}

#if defined SR_DEBUG_CONNECTION_LOGGING
#undef SR_DEBUG_CONNECTION_LOGGING
#endif

#if defined SR_DEBUG_CONNECTION
#undef SR_DEBUG_CONNECTION
#endif

#if defined SR_DEBUG_CONNECTION_THREAD
#undef SR_DEBUG_CONNECTION_THREAD
#endif


