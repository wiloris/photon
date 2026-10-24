module github.com/wiloris/photon/photon-plugin-runtime

go 1.22.2

require (
	github.com/wiloris/photon/photon-core v0.1.0
	github.com/wiloris/photon/photon-plugin-sdk v0.1.0
	go.etcd.io/bbolt v1.3.11
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/pion/dtls/v2 v2.2.12 // indirect
	github.com/pion/ice/v3 v3.0.16 // indirect
	github.com/pion/logging v0.2.3 // indirect
	github.com/pion/mdns/v2 v2.0.7 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/sctp v1.8.37 // indirect
	github.com/pion/stun/v2 v2.0.0 // indirect
	github.com/pion/transport/v2 v2.2.4 // indirect
	github.com/pion/transport/v3 v3.0.7 // indirect
	github.com/pion/turn/v3 v3.0.3 // indirect
	github.com/wlynxg/anet v0.0.3 // indirect
	golang.org/x/crypto v0.26.0 // indirect
	golang.org/x/net v0.28.0 // indirect
	golang.org/x/sys v0.23.0 // indirect
)

replace github.com/wiloris/photon/photon-core => ../photon-core

replace github.com/wiloris/photon/photon-plugin-sdk => ../photon-plugin-sdk
