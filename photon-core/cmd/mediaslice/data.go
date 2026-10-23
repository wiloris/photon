package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"
)

// Тестовые данные и НЕЗАВИСИМАЯ их проверка.
//
// Слово «независимая» здесь существенно. Клиент media считает sha256 сам, и
// сверять скачанное с тем, что он же насчитал, значило бы проверять его его
// собственным ответом. Поэтому оба хеша — исходника и скачанного — считаются
// ЗДЕСЬ, отдельным проходом по файлу на диске: crypto/sha256 поверх
// os.Open. Единственное, что берётся у клиента, — байты, которые он записал.

// sourcePrologue и sourceEpilogue — врезки, из-за которых файл невозможно
// протащить через JSON незаметно.
//
// Что в них и зачем:
//
//   - все 256 значений байта, включая NUL: JSON-строка их не переносит без
//     экранирования, а после экранирования длина и содержимое изменились бы;
//   - заведомо невалидный UTF-8: json.Marshal заменил бы его на U+FFFD, и
//     round-trip перестал бы совпадать побайтово;
//   - кавычка, обратная косая, перевод строки: минимальный набор, ломающий
//     наивную склейку;
//   - последовательность, НЕОТЛИЧИМАЯ от конверта ответа
//     ({"ok":true,...}\n) и от кадра запроса: если бы граница тела искалась
//     по содержимому, а не по объявленной длине, разбор сорвался бы именно
//     здесь.
//
// Врезки стоят и в начале, и в конце: концевая ловит потерю хвоста, которую
// начальная не увидит.
func sourcePrologue() []byte {
	var b bytes.Buffer
	for i := 0; i < 256; i++ {
		b.WriteByte(byte(i))
	}
	b.WriteString("\"\\\n\t 'quotes' and \\backslashes\\ ")
	b.Write([]byte{0x00, 0xff, 0xfe, 0xc3, 0x28, 0xa0, 0xa1, 0x80})
	b.WriteString("{\"ok\":true,\"error\":\"\",\"data\":null}\n")
	b.WriteString("{\"action\":700,\"resource\":\"media\"}\n")
	return b.Bytes()
}

func sourceEpilogue() []byte {
	var b bytes.Buffer
	b.WriteString("{\"ok\":false,\"error\":\"forbidden\"}\n")
	for i := 255; i >= 0; i-- {
		b.WriteByte(byte(i))
	}
	return b.Bytes()
}

// minSourceSize — меньше этого файл не вмещает обязательных врезок.
func minSourceSize() int64 { return int64(len(sourcePrologue()) + len(sourceEpilogue())) }

// generateSource пишет детерминированный файл заданного размера.
//
// Детерминированный, а не случайный: повторный прогон обязан давать тот же
// файл, иначе расхождение хешей между запусками невозможно отличить от
// дефекта передачи. Генератор свой (xorshift64), а не math/rand: стабильность
// последовательности здесь свойство теста, и зависеть от версии стандартной
// библиотеки она не должна.
func generateSource(path string, size int64) error {
	pro, epi := sourcePrologue(), sourceEpilogue()
	if size < int64(len(pro)+len(epi)) {
		return fmt.Errorf("mediaslice: размер %d меньше обязательных врезок (%d)",
			size, len(pro)+len(epi))
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 1<<16)
	if _, err := w.Write(pro); err != nil {
		return err
	}
	mid := size - int64(len(pro)) - int64(len(epi))
	x := uint64(0x9E3779B97F4A7C15)
	buf := make([]byte, 32<<10)
	for mid > 0 {
		n := int64(len(buf))
		if mid < n {
			n = mid
		}
		for i := int64(0); i < n; i++ {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			buf[i] = byte(x >> 33)
		}
		if _, err := w.Write(buf[:n]); err != nil {
			return err
		}
		mid -= n
	}
	if _, err := w.Write(epi); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return f.Sync()
}

// verifySourceMarkers подтверждает, что врезки на месте — и в начале, и в
// конце. Без этой проверки утверждение «байты не проходят через JSON»
// опиралось бы на намерение генератора, а не на содержимое файла.
func verifySourceMarkers(path string) error {
	pro, epi := sourcePrologue(), sourceEpilogue()
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	head := make([]byte, len(pro))
	if _, err := io.ReadFull(f, head); err != nil {
		return fmt.Errorf("начальная врезка не читается: %w", err)
	}
	if !bytes.Equal(head, pro) {
		return fmt.Errorf("начальная врезка не совпала")
	}
	st, err := f.Stat()
	if err != nil {
		return err
	}
	tail := make([]byte, len(epi))
	if _, err := f.ReadAt(tail, st.Size()-int64(len(epi))); err != nil {
		return fmt.Errorf("концевая врезка не читается: %w", err)
	}
	if !bytes.Equal(tail, epi) {
		return fmt.Errorf("концевая врезка не совпала")
	}
	return nil
}

// hashFile — независимый sha256 файла на диске плюс его длина.
//
// Длина берётся из числа прочитанных байт, а не из os.Stat: считается ровно
// то, что попало в хеш.
func hashFile(path string) (sum string, size int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// heapSampler — пик HeapAlloc за время передачи.
//
// Чем это является: измерением ТОГО ЖЕ процесса, что ведёт передачу, то есть
// клиентской половины утверждения «файл целиком в память не собирается».
// Чем НЕ является: измерением сервера. Серверная половина доказывается
// иначе — структурным тестом на запрет io.ReadAll в медийном пути — и
// смешивать одно с другим нельзя.
type heapSampler struct {
	stop chan struct{}
	done chan struct{}
	base uint64
	peak uint64
}

func startHeapSampler(interval time.Duration) *heapSampler {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	h := &heapSampler{stop: make(chan struct{}), done: make(chan struct{}), base: ms.HeapAlloc, peak: ms.HeapAlloc}
	go func() {
		defer close(h.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-h.stop:
				return
			case <-t.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > h.peak {
					h.peak = m.HeapAlloc
				}
			}
		}
	}()
	return h
}

// finish возвращает прирост пика над базой. Ноль означает «пик не превысил
// того, что было до передачи».
func (h *heapSampler) finish() uint64 {
	close(h.stop)
	<-h.done
	if h.peak <= h.base {
		return 0
	}
	return h.peak - h.base
}
