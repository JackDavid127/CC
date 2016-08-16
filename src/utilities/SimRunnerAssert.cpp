//
//  Assert.cpp
//  SimRunner
//
//  Created by Scott on 24/09/2014.
//
//

#include <stdio.h>
#include <stdarg.h>
#include <assert.h>
#include <iostream> 
#include "SimRunnerAssert.h"

#include <csignal>

namespace SimRunner
{
    namespace Utilities
    {
        void Assert(const char* file, int line, const char* stringFormat, ...)
        {
            va_list args;
            va_start(args, stringFormat);
            std::cerr << "Assertion failed at " << "FILE: " << file << ", LINE: " << line << "! Message: ";
            vfprintf(stderr, stringFormat, args);
            std::cerr << std::endl;
            va_end(args);
            
            raise(SIGINT);
        }
        
        void Assert(const char* file, int line)
        {
            Assert(file, line, "");
        }
    }
}