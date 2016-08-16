//
//  StandardErrorLogger.h
//  SimRunner
//
//  Created by Scott on 25/09/2014.
//
//

#pragma once

#include <stdio.h>
#include <cstdarg>

namespace SimRunner
{
    namespace Utilities
    {
        class StandardErrorLogger
        {
        public:
            void Log(const char* format, ...)
            {
                va_list args;
                va_start(args, format);
                vfprintf(stderr, format, args);
                va_end(args);
            }
        };
    }
}