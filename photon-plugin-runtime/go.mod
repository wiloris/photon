module github.com/wiloris/photon-plugin-runtime

go 1.22.2

require (
	github.com/wiloris/photon-core v0.0.0
	github.com/wiloris/photon-plugin-sdk v0.0.0
	go.etcd.io/bbolt v1.3.11
)

require golang.org/x/sys v0.23.0 // indirect

replace github.com/wiloris/photon-core => ../photon-core

replace github.com/wiloris/photon-plugin-sdk => ../photon-plugin-sdk
