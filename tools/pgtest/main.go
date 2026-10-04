// pgtest — временный PostgreSQL для контрактных тестов photon-quic-bridge.
//
// Только для тестов, вне production. Поднимает PostgreSQL 18.3
// (embedded-postgres: бинарники скачиваются при первом запуске и кэшируются),
// создаёт public.posts по образцу Mattermost 9.11 — колонки и индексы, на
// которые опираются checkDMSchema и тесты (каждый тест копирует структуру
// LIKE public.posts в свою временную схему), запускает переданную команду с
// PHOTON_DM_PG_DSN и останавливает базу. Каждый запуск — с чистым каталогом
// данных: повторно использованный каталог роняет запуск на «posts уже есть».
//
// Отдельный модуль (go 1.22, без зависимости от модулей дерева): собрать один
// раз, затем запускать из каталога тестируемого модуля:
//
//	(cd tools/pgtest && go build -o "${TMPDIR:-/tmp}/pgtest" .)
//	cd photon-quic-bridge && "${TMPDIR:-/tmp}/pgtest" go test -count=1 -run 'TestConv' ./server/
//
// Переменные: PGTEST_DIR — каталог базы и кэша бинарников (по умолчанию
// $TMPDIR/photon-pgtest), PGTEST_PORT — порт (по умолчанию 54331).
package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	_ "github.com/lib/pq"
)

const postsDDL = `
CREATE TABLE public.posts (
  id varchar(26) PRIMARY KEY, createat bigint, updateat bigint, editat bigint DEFAULT 0,
  deleteat bigint, ispinned boolean DEFAULT false, userid varchar(26), channelid varchar(26),
  rootid varchar(26) DEFAULT '', originalid varchar(26) DEFAULT '', message varchar(65535),
  type varchar(26), props jsonb, hashtags varchar(1000) DEFAULT '', filenames varchar(4000) DEFAULT '[]',
  fileids varchar(300) DEFAULT '[]', hasreactions boolean DEFAULT false, remoteid varchar(26),
  lastreplyat bigint DEFAULT 0);
CREATE INDEX idx_posts_channel_id_delete_at_create_at ON public.posts (channelid, deleteat, createat);
CREATE INDEX idx_posts_create_at ON public.posts (createat);
CREATE INDEX idx_posts_user_id ON public.posts (userid);`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "использование: pgtest <команда> [аргументы…]")
		os.Exit(2)
	}
	dir := os.Getenv("PGTEST_DIR")
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "photon-pgtest")
	}
	port := uint32(54331)
	if v := os.Getenv("PGTEST_PORT"); v != "" {
		p, err := strconv.ParseUint(v, 10, 16)
		if err != nil {
			fmt.Fprintln(os.Stderr, "PGTEST_PORT:", err)
			os.Exit(2)
		}
		port = uint32(p)
	}
	_ = os.RemoveAll(filepath.Join(dir, "data"))

	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Version(embeddedpostgres.V18).Port(port).
		RuntimePath(filepath.Join(dir, "run")).DataPath(filepath.Join(dir, "data")).
		CachePath(filepath.Join(dir, "cache")).BinariesPath(filepath.Join(dir, "bin")).
		Logger(nil))
	if err := pg.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "pgtest: запуск PostgreSQL:", err)
		os.Exit(2)
	}
	code := 0
	defer func() { _ = pg.Stop(); os.Exit(code) }()

	dsn := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", port)
	db, err := sql.Open("postgres", dsn)
	if err == nil {
		_, err = db.Exec(postsDDL)
		db.Close()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgtest: public.posts:", err)
		code = 2
		return
	}
	cmd := exec.Command(os.Args[1], os.Args[2:]...)
	cmd.Env = append(os.Environ(), "PHOTON_DM_PG_DSN="+dsn)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		code = 1
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
	}
}
