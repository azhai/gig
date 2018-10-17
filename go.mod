module github.com/azhai/gig

replace (
	golang.org/x/crypto => github.com/golang/crypto v0.0.0-20181015023909-0c41d7ab0a0e
	golang.org/x/net => github.com/golang/net v0.0.0-20181011144130-49bb7cea24b1
	golang.org/x/sync => github.com/golang/sync v0.0.0-20180314180146-1d60e4601c6f
	golang.org/x/sys => github.com/golang/sys v0.0.0-20181011152604-fa43e7bc11ba
	golang.org/x/text => github.com/golang/text v0.3.0
)

require (
	github.com/boltdb/bolt v1.3.1
	github.com/recoilme/slowpoke v0.0.0-20180829192753-92804a51a196
)
