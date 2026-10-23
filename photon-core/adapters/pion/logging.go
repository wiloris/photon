package pion

import "github.com/pion/logging"

// nopLoggerFactory глушит логи Pion: у ядра нет порта логирования, и писать в
// stderr из адаптера значило бы решать за вызывающего. Диагностика приходит
// через ошибки, а не через чужой логгер.
//
// Задаётся ЯВНО каждому из трёх слоёв — ICE, DTLS, SCTP. Нулевое поле
// LoggerFactory означает у Pion не «молчать», а NewDefaultLoggerFactory с
// Writer = os.Stderr; пропуск в одном слое ничего не ломает в работе и виден
// только по неожиданному выводу в потоке чужого процесса. Проверяется
// TestEveryPionLayerGetsSilentLogger — комментарий такую пропажу не ловит,
// что и подтвердил аудит: до него логгер стоял на одном слое из трёх.
type nopLoggerFactory struct{}

func (nopLoggerFactory) NewLogger(string) logging.LeveledLogger { return nopLogger{} }

type nopLogger struct{}

func (nopLogger) Trace(string)                  {}
func (nopLogger) Tracef(string, ...interface{}) {}
func (nopLogger) Debug(string)                  {}
func (nopLogger) Debugf(string, ...interface{}) {}
func (nopLogger) Info(string)                   {}
func (nopLogger) Infof(string, ...interface{})  {}
func (nopLogger) Warn(string)                   {}
func (nopLogger) Warnf(string, ...interface{})  {}
func (nopLogger) Error(string)                  {}
func (nopLogger) Errorf(string, ...interface{}) {}
