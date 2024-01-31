## build: Builds a custom 'k6' with the local extension.
build:
	go install go.k6.io/xk6/cmd/xk6@latest
	xk6 build --with $(shell go list -m)=.
