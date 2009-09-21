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

package example;

import com.gist.twitter.TwitterClient;
import com.gist.twitter.TwitterIdFetcher;
import com.gist.twitter.TwitterStreamProcessor;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;;

class Example {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println(
                "Usage: Example username:password twitter_id ...");
            System.exit(1);
        }

        Collection<String> credentials = new ArrayList<String>();
        credentials.add(args[0]);

        final Collection<String> ids = new HashSet<String>();
        for (int i = 1; i < args.length; i++) {
            ids.add(args[i]);
        }
        TwitterIdFetcher twitterIdFetcher = new TwitterIdFetcher() {
            public Collection<String> getIds() {
                return ids;
            }
        };

        new TwitterClient(
            twitterIdFetcher,
            new ExampleTwitterStreamProcessor(),
            "http://stream.twitter.com/1/statuses/filter.json",
            200,
            credentials,
            60 * 1000L).execute();
    }

    /**
     * Example TwitterStreamProcessor that uses org.json.* to process the
     * stream and just prints out each tweet.  This isn't an endorsement
     * of any techniques, just an example.  In real life the tweet would
     * likely be put into some kind of queue system.
     */
    private static class ExampleTwitterStreamProcessor
        implements TwitterStreamProcessor {
        public void processTwitterStream(InputStream is, String credentials,
            HashSet<String> ids)
            throws InterruptedException, IOException {

            JSONTokener jsonTokener = new JSONTokener(
                new InputStreamReader(is, "UTF-8"));
            while (true) {
                try {
                    JSONObject jsonObject = new JSONObject(jsonTokener);
                    System.out.println("Got " + jsonObject);
                }
                catch (JSONException ex) {
                    throw new IOException(
                        "Got JSONException: " + ex.getMessage());
                }
            }
        }
    }
}