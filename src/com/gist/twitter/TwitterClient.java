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

    private final TwitterIdFetcher twitterIdFetcher;
    private final TwitterStreamProcessor twitterStreamProcessor;
    private final String baseUrl;
    private final int maxIdsPerCredentials;
    private final Collection<UsernamePasswordCredentials> credentials;
    private final long processForMillis;

    private final AuthScope authScope;

    /**
     * Constructs a TwitterClient.
     *
     * @param twitterIdFetcher used to get twitter ids to process.  The
     *   getIds() method will be called periodically to refresh the ids.
     * @param twitterStreamProcessor processes the twitter stream
     * @param baseUrl url of the twitter stream
     * @param maxIdsPerCredentials maximum number of twitter ids we
     *   can follow with one set of credentials
     * @param credentials credentials to connect with, in the form
     *   "username:password".  Multiple credentials can be used to follow
     *   large numbers of twitter ids.
     * @param processForMillis how long to process before refreshing the
     *   twitter ids and reconnecting.
     */
    public TwitterClient(
        TwitterIdFetcher twitterIdFetcher,
        TwitterStreamProcessor twitterStreamProcessor,
        String baseUrl,
        int maxIdsPerCredentials,
        Collection<String> credentials,
        long processForMillis) {

        this.twitterIdFetcher = twitterIdFetcher;
        this.twitterStreamProcessor = twitterStreamProcessor;
        this.baseUrl = baseUrl;
        this.maxIdsPerCredentials = maxIdsPerCredentials;
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
        Collection<String> ids = twitterIdFetcher.getIds();

        // Distribute ids somewhat evenly among credentials to test
        // multiple threads.
        int batchSize = Math.min(ids.size() / credentials.size() + 1,
            maxIdsPerCredentials);

        Collection<Thread> threads = new ArrayList<Thread>();

        Iterator<String> idIterator = ids.iterator();
        for (UsernamePasswordCredentials upc : credentials) {
            HashSet<String> idBatch = new HashSet<String>(batchSize);
            for (int i = 0; i < batchSize && idIterator.hasNext(); i++) {
                idBatch.add(idIterator.next());
            }

            if (idBatch.isEmpty()) {
                break;
            }

            Thread t = new Thread(
                new TwitterProcessor(upc, idBatch),
                "Twitter download as " + upc
                + " (" + threadCount.getAndIncrement() + ")");
            threads.add(t);
            t.start();
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

        public TwitterProcessor  (
            UsernamePasswordCredentials credentials, HashSet<String> ids) {
            this.credentials = credentials;
            this.ids = ids;
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
                    catch (InterruptedException ex) {
                        // Don't let this be handled as a generic Exception.
                        return;
                    }
                    catch (InterruptedIOException ex) {
                        return;
                    }
                    catch (HttpException ex) {
                        logger.log(Level.WARNING,
                            credentials + ": Error fetching from " + baseUrl,
                            ex);
                        httpBackOff.backOff();
                    }
                    catch (IOException ex) {
                        logger.log(Level.WARNING,
                            credentials + ": Error fetching from " + baseUrl,
                            ex);
                        tcpBackOff.backOff();
                    }
                    catch (Exception ex) {
                        // This could be a NumberFormatException or
                        // something.  Open a new connection to
                        // resync.
                        logger.log(Level.WARNING,
                            credentials + ": Error fetching from " + baseUrl,
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
            postMethod.setRequestBody(buildBody(ids));

            logger.info(credentials + ": Connecting to " + baseUrl);
            httpClient.executeMethod(postMethod);
            try {
                if (postMethod.getStatusCode() != HttpStatus.SC_OK) {
                    throw new HttpException(
                        "Got status " + postMethod.getStatusCode());
                }
                InputStream is = postMethod.getResponseBodyAsStream();
                // We've got a successful connection.
                resetBackOff();
                logger.info(credentials + ": Processing from " + baseUrl);
                twitterStreamProcessor.processTwitterStream(
                    is, credentials.toString(), ids);
                logger.info(credentials + ": Completed processing from "
                    + baseUrl);
            } finally {
                // Abort the method, otherwise releaseConnection() will
                // attempt to finish reading the never-ending response.
                // These methods do not throw exceptions.
                postMethod.abort();
                postMethod.releaseConnection();
            }
        }

        private NameValuePair[] buildBody(Iterable<String> ids) {
            StringBuilder sb = new StringBuilder();
            boolean needComma = false;
            for (String id : ids) {
                if (needComma) {
                    sb.append(',');
                }
                needComma = true;
                sb.append(id);
            }
            return new NameValuePair[] {
                new NameValuePair("follow", sb.toString()),
            };
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
