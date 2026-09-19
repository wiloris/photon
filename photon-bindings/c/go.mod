// Модуль bindings — отдельный от photon-core: у него своя граница (cgo,
// -buildmode=c-shared) и свои зависимости, а Core не должен зависеть от него
// никогда.
//
// replace обязателен и не является временным костылём: photon-core объявлен
// версией v0.0.0, которой нет ни в одном registry, и появится она там не
// раньше первого релиза платформы. Без replace модуль не резолвится.
//
// Путь ОТНОСИТЕЛЬНЫЙ, и это главное требование к этой строке. Прежний
// go.mod.example содержал абсолютные пути машины, на которой велась
// разработка (/home/claude/...), — на любой другой машине это мусор, а
// выглядело рабочим до первой попытки собрать. Тот же класс, что незакрытый
// перенос ADR-025.
module github.com/wiloris/photon-bindings-c

go 1.22.2

require github.com/wiloris/photon-core v0.0.0

require (
	github.com/go-task/slim-sprig v0.0.0-20230315185526-52ccab3ef572 // indirect
	github.com/google/pprof v0.0.0-20210407192527-94a9f03dee38 // indirect
	github.com/onsi/ginkgo/v2 v2.9.5 // indirect
	github.com/quic-go/quic-go v0.48.2 // indirect
	go.etcd.io/bbolt v1.3.11 // indirect
	go.uber.org/mock v0.4.0 // indirect
	golang.org/x/crypto v0.26.0 // indirect
	golang.org/x/exp v0.0.0-20240506185415-9bf2ced13842 // indirect
	golang.org/x/mod v0.17.0 // indirect
	golang.org/x/net v0.28.0 // indirect
	golang.org/x/sys v0.23.0 // indirect
	golang.org/x/tools v0.21.1-0.20240508182429-e35e4ccd0d2d // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)

replace github.com/wiloris/photon-core => ../../photon-core
