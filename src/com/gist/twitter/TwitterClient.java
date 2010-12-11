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
import java.io.InterruptedIOException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.HttpURL;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

/**
 * Connects to the Twitter streaming API using one or more sets of
 * credentials and hands the streams off for processing.  Backs off
 * and reconnects on exceptions.  Reconnects periodically to allow the
 * set of twitter ids to change.
 *
 * See the spec at http://apiwiki.twitter.com/Streaming-API-Documentation.
 *
 * @author <a href="mailto:tom@gist.com">Tom May</a>
 */
public class TwitterClient {
    private static Logger logger =
        Logger.getLogger(TwitterClient.class.getName());

    // For generating unique thread names for logging and thread dumps.
    private AtomicInteger threadCount = new AtomicInteger(0);

    private final FilterParameterFetcher filterParameterFetcher;
    private final TwitterStreamProcessor twitterStreamProcessor;
    private final String baseUrl;
    private final int maxFollowIdsPerCredentials;
    private final int maxTrackKeywordsPerCredentials;
    private final Collection<UsernamePasswordCredentials> credentials;
    private final long processForMillis;

    private final AuthScope authScope;

    /**
     * Constructs a TwitterClient.
     *
     * @param filterParameterFetcher used to get twitter ids to
     *   follow.  The getFollowIds() and getTrackKeywords() methods
     *   will be called periodically to refresh the ids and keywords.
     * @param twitterStreamProcessor processes the twitter stream
     * @param baseUrl url of the twitter stream
     * @param maxFollowIdsPerCredentials maximum number of twitter ids
     *   we can follow with one set of credentials
     * @param maxTrackKeywordsPerCredentials maximum number of
     *   keywords we can track with one set of credentials
     * @param credentials credentials to connect with, in the form
     *   "username:password".  Multiple credentials can be used to follow
     *   large numbers of twitter ids.
     * @param processForMillis how long to process before refreshing the
     *   twitter ids and reconnecting.
     */
    public TwitterClient(
        FilterParameterFetcher filterParameterFetcher,
        TwitterStreamProcessor twitterStreamProcessor,
        String baseUrl,
        int maxFollowIdsPerCredentials,
        int maxTrackKeywordsPerCredentials,
        Collection<String> credentials,
        long processForMillis) {

        this.filterParameterFetcher = filterParameterFetcher;
        this.twitterStreamProcessor = twitterStreamProcessor;
        this.baseUrl = baseUrl;
        this.maxFollowIdsPerCredentials = maxFollowIdsPerCredentials;
        this.maxTrackKeywordsPerCredentials = maxTrackKeywordsPerCredentials;
        this.credentials = createCredentials(credentials);
        this.processForMillis = processForMillis;

        try {
            authScope = createAuthScope(baseUrl);
        }
        catch (URIException ex) {
            throw new IllegalArgumentException("Invalid url: " + baseUrl, ex);
        }
    }

    /**
     * Fetches twitter ids, connects to the twitter api stream, and
     * processes the stream.  Repeats every processForMillis.
     */
    public void execute() {
        while (true) {
            processForATime();
        }
    }

    /**
     * Turns a collection of "username:password" credentials into a collection
     * of UsernamePasswordCredentials for use with HttpClient.
     */
    private Collection<UsernamePasswordCredentials> createCredentials(
        Collection<String> logins) {
        ArrayList<UsernamePasswordCredentials> result =
            new ArrayList<UsernamePasswordCredentials>();
        for (String login : logins) {
            result.add(new UsernamePasswordCredentials(login));
        }
        return result;
    }

    /**
     * Extracts the host and post from the baseurl and constructs an
     * appropriate AuthScope for them for use with HttpClient.
     */
    private AuthScope createAuthScope(String baseUrl) throws URIException {
        HttpURL url = new HttpURL(baseUrl);
        return new AuthScope(url.getHost(), url.getPort());
    }

    /**
     * Divides the ids among the credentials, and starts up a thread
     * for each set of credentials with a TwitterProcessor that
     * connects to twitter, and reconnects on exceptions, and
     * processes the stream.  After processForMillis, interrupt the
     * threads and return.
     */
    private void processForATime() {
        Collection<String> followIds =
            filterParameterFetcher.getFollowIds();
        Collection<HashSet<String>> followIdSets =
            createSets(followIds, maxFollowIdsPerCredentials);

        Collection<String> trackKeywords =
            filterParameterFetcher.getTrackKeywords();
        Collection<HashSet<String>> trackKeywordSets =
            createSets(trackKeywords, maxTrackKeywordsPerCredentials);

        Collection<Thread> threads = new ArrayList<Thread>();

        Iterator<UsernamePasswordCredentials> credentialsIterator =
            credentials.iterator();

        for (HashSet<String> ids : followIdSets) {
            for (Collection<String> keywords : trackKeywordSets) {
                if (credentialsIterator.hasNext()) {
                    UsernamePasswordCredentials upc =
                        credentialsIterator.next();
                    Thread t = new Thread(
                        new TwitterProcessor(upc, ids, keywords),
                        "Twitter download as " + upc.getUserName()
                        + " (" + threadCount.getAndIncrement() + ")");
                    threads.add(t);
                    t.start();
                }
                else {
                    logger.warning(
                        "Out of credentials, ignoring some ids/keywords.");
                }
            }
        }

        try {
            Thread.sleep(processForMillis);
        }
        catch (InterruptedException ex) {
            // Won't happen, ignore.
        }

        for (Thread t : threads) {
            t.interrupt();
        }

        // It doesn't matter so much whether the threads exit in a
        // timely manner.  We'll just get some IOExceptions or
        // something and retry.  This just makes the logs a little
        // nicer since we won't usually start a thread until the old
        // one has exited.
        for (Thread t : threads) {
            try {
                t.join(1000L);
            }
            catch (InterruptedException ex) {
                // Won't happen.
            }
        }
    }

    /**
     * Divides the given collection of items into collections of at
     * most maxPerSet.  If items is empty, an empty collection will be
     * returned.  If items is null, a collection with a single null
     * element will be returned.
     */
    private Collection<HashSet<String>> createSets(
        Collection<String> items, int maxPerSet)
    {
        Collection<HashSet<String>> sets = new ArrayList<HashSet<String>>();

        if (items == null) {
            sets.add(null);
            return sets;
        }

        HashSet<String> set = null;
        for (String item : items) {
            if (set == null) {
                set = new HashSet<String>();
                sets.add(set);
            }
            set.add(item);
            if (set.size() >= maxPerSet) {
                set = null;
            }
        }

        return sets;
    }

    /**
     * Handles a twitter connection for one set of credentials.  Runs
     * in a separate thread, connecting, reconnecting, and processing
     * until interrupted.
     */
    private class TwitterProcessor implements Runnable {
        // The backoff behavior is from the spec.
        private final BackOff tcpBackOff = new BackOff(true, 250, 16000);
        private final BackOff httpBackOff = new BackOff(10000, 240000);

        private final UsernamePasswordCredentials credentials;
        private final HashSet<String> ids;
        private final Collection<String> keywords;

        public TwitterProcessor  (
            UsernamePasswordCredentials credentials, HashSet<String> ids,
            Collection<String> keywords)
        {
            this.credentials = credentials;
            this.ids = ids;
            this.keywords = keywords;
        }

        /**
         * Connects to twitter and processes the streams.  On
         * exception, backs off and reconnects.  Runs until the thread
         * is interrupted.
         */
        //@Override
        public void run() {
            logger.info("Begin " + Thread.currentThread().getName());
            try {
                while (true) {
                    if (Thread.interrupted()) {
                        return;
                    }
                    try {
                        connectAndProcess();
                    }
                    catch (SocketTimeoutException ex) {
                        // Handle like an IOException even though it's
                        // an InterruptedIOException.
                        logger.log(Level.WARNING,
                                   credentials.getUserName()
                                   + ": Error fetching from " + baseUrl,
                                   ex);
                        tcpBackOff.backOff();
                    }
                    catch (InterruptedException ex) {
                        // Don't let this be handled as a generic Exception.
                        return;
                    }
                    catch (InterruptedIOException ex) {
                        return;
                    }
                    catch (HttpException ex) {
                        logger.log(Level.WARNING,
                                   credentials.getUserName() 
                                   + ": Error fetching from " + baseUrl,
                                   ex);
                        httpBackOff.backOff();
                    }
                    catch (IOException ex) {
                        logger.log(Level.WARNING,
                                   credentials.getUserName()
                                   + ": Error fetching from " + baseUrl,
                                   ex);
                        tcpBackOff.backOff();
                    }
                    catch (Exception ex) {
                        // This could be a NumberFormatException or
                        // something.  Open a new connection to
                        // resync.
                        logger.log(Level.WARNING,
                                   credentials.getUserName() 
                                   + ": Error fetching from " + baseUrl,
                                   ex);
                    }
                }
            }
            catch (InterruptedException ex) {
                return;
            }
            finally {
                logger.info("End " + Thread.currentThread().getName());
            }
        }

        /**
         * Connects to twitter and handles tweets until it gets an
         * exception or is interrupted.
         */
        private void connectAndProcess()
            throws HttpException, InterruptedException, IOException {
            HttpClient httpClient = new HttpClient();

            // HttpClient has no way to set SO_KEEPALIVE on our
            // socket, and even if it did the TCP keepalive interval
            // may be too long, so we need to set a timeout at this
            // level.  Twitter will send periodic newlines for
            // keepalive if there is no traffic, but they don't say
            // how often.  Looking at the stream, it's every 30
            // seconds, so we use a read timeout of twice that.

            httpClient.getHttpConnectionManager().getParams()
                .setSoTimeout(60000);

            // Don't retry, we want to handle the backoff ourselves.
            httpClient.getParams().setParameter(
                HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(0, false));

            httpClient.getState().setCredentials(authScope, credentials);
            httpClient.getParams().setAuthenticationPreemptive(true);

            PostMethod postMethod = new PostMethod(baseUrl);
            postMethod.setRequestBody(makeRequestBody());

            logger.info(credentials.getUserName() 
                        + ": Connecting to " + baseUrl);
            httpClient.executeMethod(postMethod);
            try {
                if (postMethod.getStatusCode() != HttpStatus.SC_OK) {
                    throw new HttpException(
                        "Got status " + postMethod.getStatusCode());
                }
                InputStream is = postMethod.getResponseBodyAsStream();
                // We've got a successful connection.
                resetBackOff();
                logger.info(credentials.getUserName() 
                            + ": Processing from " + baseUrl);
                twitterStreamProcessor.processTwitterStream(
                    is, credentials.toString(), ids);
                logger.info(credentials.getUserName() 
                            + ": Completed processing from "
                            + baseUrl);
            } finally {
                // Abort the method, otherwise releaseConnection() will
                // attempt to finish reading the never-ending response.
                // These methods do not throw exceptions.
                postMethod.abort();
                postMethod.releaseConnection();
            }
        }

        private NameValuePair[] makeRequestBody() {
            Collection<NameValuePair> params = new ArrayList<NameValuePair>();
            if (ids != null) {
                params.add(createNameValuePair("follow", ids));
            }
            if (keywords != null) {
                params.add(createNameValuePair("track", keywords));
            }
            if (twitterStreamProcessor.consumesDelimitedStream()) {
                params.add(new NameValuePair("delimited", "length"));
            }
            return params.toArray(new NameValuePair[params.size()]);
        }

        private NameValuePair createNameValuePair(
            String name, Collection<String> items)
        {
            StringBuilder sb = new StringBuilder();
            boolean needComma = false;
            for (String item : items) {
                if (needComma) {
                    sb.append(',');
                }
                needComma = true;
                sb.append(item);
            }
            return new NameValuePair(name, sb.toString());
        }

        private void resetBackOff() {
            tcpBackOff.reset();
            httpBackOff.reset();
        }
    }

    /**
     * Handles backing off for an initial time, doubling until a cap
     * is reached.
     */
    private static class BackOff {
        private final boolean noInitialBackoff;
        private final long initialMillis;
        private final long capMillis;
        private long backOffMillis;

        /**
         * @param noInitialBackoff true if the initial backoff should be zero
         * @param initialMillis the initial amount of time to back off, after
         *   an optional zero-length initial backoff
         * @param capMillis upper limit to the back off time
         */
        public BackOff(
            boolean noInitialBackoff, long initialMillis, long capMillis) {
            this.noInitialBackoff = noInitialBackoff;
            this.initialMillis = initialMillis;
            this.capMillis = capMillis;
            reset();
        }

        public BackOff(long initialMillis, long capMillis) {
            this(false, initialMillis, capMillis);
        }

        public void reset() {
            if (noInitialBackoff) {
                backOffMillis = 0;
            }
            else{
                backOffMillis = initialMillis;
            }
        }

        public void backOff() throws InterruptedException {
            if (backOffMillis == 0) {
                backOffMillis = initialMillis;
            }
            else {
                Thread.sleep(backOffMillis);
                backOffMillis *= 2;
                if (backOffMillis > capMillis) {
                    backOffMillis = capMillis;
                }
            }
        }
    }
}
