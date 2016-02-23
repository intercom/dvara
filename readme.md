dvara [![Build Status](https://secure.travis-ci.org/intercom/dvara.png)](http://travis-ci.org/intercom/dvara)
=====

dvara provides a connection pooling proxy for
[MongoDB](http://www.mongodb.org/). For more information look at the associated
blog post: http://blog.parse.com/2014/06/23/dvara/.

To build from source you'll need [Go](http://golang.org/).
Install the app, with latest version

    go get github.com/intercom/dvara

Run the proxy, assuming the mongodb is already running

    dvara -addrs=$HOST:$PORT where host and port is location of mongo db instance

Library documentation: https://godoc.org/github.com/facebook/dvara
