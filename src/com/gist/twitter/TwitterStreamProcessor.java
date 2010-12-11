/*
 * Copyright 2009 Gist, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gist.twitter;

import java.io.InputStream;
import java.io.IOException;
import java.util.HashSet;

/**
 * @author <a href="mailto:tom@gist.com">Tom May</a>
 */
public interface TwitterStreamProcessor {
    /**
     * Processes the twitter stream until it's interrupted or gets an
     * IOException.  This method should expect to be interrupted, and
     * throw an InterruptedExcpetion or InterruptedIOException.
     *
     * @param is the stream to process
     * @param credentials the credentials used to create the stream,
     *   for logging purposes
     * @param ids the twitter ids this stream is following
     */
    void processTwitterStream(InputStream is, String credentials,
        HashSet<String> ids)
        throws InterruptedException, IOException;

    /**
     * Returns true if it this processor consumes a delimited stream
     */
    boolean consumesDelimitedStream();
}