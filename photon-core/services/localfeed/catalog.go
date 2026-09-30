package localfeed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/wiloris/photon-core/ports"
)

// Каталог каналов и курсор синхронизации.
//
// Каталог отвечает на вопрос, которого у ленты одного канала не было: какие
// каналы есть и где у каждого самая свежая запись. Без него список
// разговоров строился бы полным обходом namespace — записи разных каналов
// перемежаются в пространстве ключей, и перечислить каналы иначе нельзя.
//
// Курсор — непрозрачная строка сервера, с которой продолжать синхронизацию
// канала. Хранится рядом с сегментами и пишется только вместе с блоком
// (IngestWith): отдельного «сохранить курсор» нет намеренно.
//
// Раскладка ключей в namespace photon.localfeed:
//
//	<channel> 0x00 …        записи (localfeed.go)
//	<channel> 0x01          сегменты (segments.go)
//	<channel> 0x02          курсор
//	0x00 'c' <channel>      запись каталога
//
// Ключ каталога начинается с 0x00 и потому не пересекается с ключами
// канала: идентификатор канала непуст (ErrEmptyChannel) и байта 0x00 не
// содержит (ErrInvalidChannelID, ADR-095 п. 2.8).

// ErrEmptyCursor — курсор задан, но пуст. «Очистить курсор» и «не менять
// курсор» — разные намерения; второе выражается nil, первого нет.
var ErrEmptyCursor = errors.New("localfeed: cursor must be non-empty when set")

func cursorKey(channelID string) []byte {
	b := make([]byte, 0, len(channelID)+1)
	b = append(b, channelID...)
	return append(b, 0x02)
}

var catalogPrefix = []byte{0x00, 'c'}

func catalogKey(channelID string) []byte {
	return append(append([]byte(nil), catalogPrefix...), channelID...)
}

// CatalogEntry — канал и позиция его самой свежей записи.
type CatalogEntry struct {
	ChannelID string   `json:"channel_id"`
	Newest    Position `json:"newest"`
}

// touchCatalog поднимает позицию канала в каталоге, если среди recs есть
// запись новее известной. Записи ленты не удаляются (ADR-054), поэтому
// позиция только растёт.
func touchCatalog(tx ports.WriteTx, channelID string, recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	top := Position{OrderKey: recs[0].OrderKey, ID: recs[0].ID}
	for _, r := range recs[1:] {
		top = newest(top, Position{OrderKey: r.OrderKey, ID: r.ID})
	}
	k := catalogKey(channelID)
	v, err := tx.Get(Namespace, k)
	switch {
	case err == nil:
		var cur Position
		if err := json.Unmarshal(v, &cur); err != nil {
			return fmt.Errorf("localfeed: decode catalog %q: %w", channelID, err)
		}
		if !top.newerThan(cur) {
			return nil
		}
	case !errors.Is(err, ports.ErrNotFound):
		return err
	}
	enc, err := json.Marshal(top)
	if err != nil {
		return fmt.Errorf("localfeed: encode catalog: %w", err)
	}
	return tx.Put(Namespace, k, enc)
}

// Catalog — все каналы с записями, самые свежие первыми (порядок — тот же
// замороженный Position.newerThan, ADR-052).
func (s *Store) Catalog(ctx context.Context) ([]CatalogEntry, error) {
	var out []CatalogEntry
	err := s.st.View(ctx, func(tx ports.ReadTx) error {
		return tx.ForEachPrefixFrom(Namespace, catalogPrefix, nil, func(k, v []byte) error {
			var p Position
			if err := json.Unmarshal(v, &p); err != nil {
				return fmt.Errorf("localfeed: decode catalog: %w", err)
			}
			out = append(out, CatalogEntry{ChannelID: string(k[len(catalogPrefix):]), Newest: p})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Newest.newerThan(out[j].Newest) })
	return out, nil
}

// Cursor — сохранённый курсор синхронизации канала. found = false — курсора
// ещё не было: синхронизация начинается без него.
func (s *Store) Cursor(ctx context.Context, channelID string) ([]byte, bool, error) {
	if err := validChannel(channelID); err != nil {
		return nil, false, err
	}
	var (
		out   []byte
		found bool
	)
	err := s.st.View(ctx, func(tx ports.ReadTx) error {
		v, err := tx.Get(Namespace, cursorKey(channelID))
		if errors.Is(err, ports.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		out, found = append([]byte(nil), v...), true
		return nil
	})
	return out, found, err
}
