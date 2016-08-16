//
//  CausalRelationship.h
//  SimRunner
//
//  Created by Scott on 15/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            enum CausalRelationship
            {
                Equal=1,
                HappensBefore=2,
                HappensAfter=3,
                Concurrent=4
            };
        }
    }
}
