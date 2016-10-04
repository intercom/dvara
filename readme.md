dvara [![Build Status](https://secure.travis-ci.org/intercom/dvara.png)](http://travis-ci.org/intercom/dvara)
=====

Dvara provides a connection pooling proxy for [MongoDB](http://www.mongodb.org/).

github.com/intercom/dvara is a fork of the [original package](http://blog.parse.com/2014/06/23/dvara/.), with significant changes to how state changes are handled.

Library documentation: https://godoc.org/github.com/intercom/dvara

## Changing dvara
### Installing locally
```bash
# checkout to your GOPATH
git clone git@github.com:intercom/dvara.git src/github.com/intercom/dvara
cd src/github.com/intercom/dvara
go install cmd/dvara
```

### Testing that it works
```bash
ssh <machine-hostname>
cd /home/ec2-user/src/github.com/intercom/dvara
export GOPATH=~
git fetch && git checkout <branch>
go install cmd/dvara
sudo monit status | grep Process | grep dvara | cut -d"'" -f 2 | xargs -n 1 sudo monit restart
# verify that Earth still turns around by tailing dvara_shared.log and production.log
```

