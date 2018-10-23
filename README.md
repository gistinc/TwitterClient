# This is a Java client for the Twitter streaming API, documented at

![alt text](https://help.twitter.com/content/dam/help-twitter/twitter_logo_blue.png)

See [Streaming API Documentation](http://apiwiki.twitter.com/Streaming-API-Documentation)

## Source is available at
[Github](http://github.com/gistinc/TwitterClient)

### Description
com.gist.twitter.TwitterClient connects to Twitter using the Jakarta
Commons HttpClient 3.1, [official doc](http://hc.apache.org/httpclient-3.x/) . 
It backs off and reconnects on HTTP and TCP errors as per the spec. 
It can connect using multiple sets of credentials at once.

TwitterClient is constructed with two helper objects.  This
functionality has been factored out since it's likely to vary wildly
according to need.

- A FilterParameterFetcher is called periodically to get the set of
  Twitter ids to follow and keywords to track.
- A TwitterStreamProcessor processes the actual data stream.  How this
  is done and how you filter and process the results are up to you.
  Twitter recommends handing off statuses via an asynchronous queueing
  mechanism, which is a good design.  If multiple credentials are
  used, all streams use the same TwitterStreamProcessor object which
  allows the content of the streams to be aggregated.

The example code provides rudimentary implementations of both of these
objects.  It will connect with the given credentials, track a given
set of Twitter ids, parse the JSON, and print it to stdout.
Essentially this just replicates the stream to stdout.