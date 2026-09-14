# photon-bindings/jvm

**Общий JVM-мост** к Photon C ABI v1 + Kotlin-обёртка: один и тот же
`photon_jni.c`, один и тот же Java/Kotlin-код обслуживают и Desktop JVM, и
будущий Android. Каталог назывался `android/` до ADR-051 — имя перестало
соответствовать содержимому, когда мост собрался обычным `cc` и прошёл тесты
на десктопной JVM. Дублирующего `desktop/`-моста нет и не заводится: маршалинг
у платформ один, а различаются вещи вроде хранения ключа средствами ОС —
для них подкаталог появится тогда, когда появится код (ADR-051, ADR-021).

## Граница ABI (жёсткое правило)

```
Kotlin / Java
      │
      ▼
    JNI (photon_jni.c)     ← тонкий маршалинг, без бизнес-логики
      │
      ▼
 photon_core.h             ← единственная граница, ADR-020/026/027/028
      │
      ▼
 libphoton_core.so
      │
      ▼
 photon-core (Go)
```

Мост **не** обращается к Go-символам, внутренним структурам `photon-core`
или деталям реализации cgo. Проверено машинно:

```
$ nm -D libphoton_jni.so | grep -E " U (go\.|runtime\.|_cgo)"
(пусто)

$ nm -D libphoton_jni.so | grep " U Photon_"
U Photon_Create, Photon_Destroy, Photon_FreeString,
U Photon_GetABIVersion, Photon_GetLastError, Photon_SendAction,
U Photon_StringData
```

## Файлы

| Файл | Назначение |
|---|---|
| `java/dev/photon/core/PhotonNative.java` | Java API с `native`-методами, зеркалит C ABI один в один |
| `jni/dev_photon_core_PhotonNative.h` | Сгенерирован `javac -h` — канонические JNI-сигнатуры от самого JDK |
| `jni/photon_jni.c` | C-мост. Compile-time проверка соответствия кодов `PhotonResult` Java↔C |
| `java/PhotonJniTest.java` | E2E-тест на JVM, 32 проверки |
| `kotlin/Photon.kt` | Идиоматичная Kotlin-обёртка: `Closeable`, исключения, проверка ABI MAJOR |
| `kotlin/PhotonKotlinTest.kt` | E2E-тест Kotlin-обёртки, 14 проверок |

## Сборка

Все шаги — от корня репозитория, каждый в своей подоболочке: `cd` внутри
`(...)` не переживает скобку, поэтому шаги независимы и порядок `cd` не
накапливается.

```sh
# 1. Собрать C ABI библиотеку — ТОЛЬКО в build/
(cd photon-bindings/c && go build -buildmode=c-shared -o build/libphoton_core.so .)

# 2. Сгенерировать JNI-заголовок
(cd photon-bindings/jvm/java && javac -h ../jni -d /tmp/out dev/photon/core/PhotonNative.java)

# 3. Собрать JNI-мост
#    -I../../c обязателен: photon_jni.c включает "photon_core.h" — ЗАМОРОЖЕННЫЙ
#    контракт, а он лежит только там. Библиотека берётся из build/, заголовок —
#    из исходников; это разные каталоги, и путать их нельзя
(cd photon-bindings/jvm/jni && JAVA_INC=$JAVA_HOME/include && \
 gcc -shared -fPIC -I"$JAVA_INC" -I"$JAVA_INC/linux" -I. -I../../c \
     -o /tmp/libphoton_jni.so photon_jni.c \
     -L../../c/build -l:libphoton_core.so \
     -Wl,-rpath,"$PWD/../../c/build" -Wall -Wextra)

# 4. Тесты
javac -d /tmp/jnitest photon-bindings/jvm/java/PhotonJniTest.java \
                      photon-bindings/jvm/java/dev/photon/core/PhotonNative.java
java -cp /tmp/jnitest PhotonJniTest /tmp/libphoton_jni.so

kotlinc photon-bindings/jvm/kotlin/Photon.kt \
        photon-bindings/jvm/kotlin/PhotonKotlinTest.kt \
        photon-bindings/jvm/java/dev/photon/core/PhotonNative.java \
        -include-runtime -d /tmp/kt.jar
java -cp /tmp/kt.jar:/tmp/jnitest PhotonKotlinTestKt /tmp/libphoton_jni.so
```

**Почему шаг 1 собирает в `build/`, а не рядом с исходниками.**
`-buildmode=c-shared` всегда генерирует C-заголовок рядом с выходным файлом и
называет его по имени `-o`. Прежняя редакция (`-o libphoton_core.so` прямо в
`photon-bindings/c`) роняла `libphoton_core.h` в каталог с исходниками — рядом
с `photon_core.h`, замороженным контрактом ABI. Перепутать два заголовка,
отличающихся префиксом `lib`, легко, а сгенерённый ещё и не отслеживается
`.gitignore` (там только `*.so`), то есть попадает в `git status` как новый
файл. Соседний README (`photon-bindings/c/README.md`) описывает более острый
случай той же механики: сборка с `-o photon_core.so` затирает сам
`photon_core.h`.

**Артефакты сборки — вне репозитория** (`/tmp`, `build/`): JNI-мост и тестовые
классы к исходникам не относятся и в дереве им делать нечего.

**Шаг 3 в прежней редакции не мог собраться вообще.** Флаги были
`-I"$JAVA_INC" -I"$JAVA_INC/linux" -I.`, то есть каталог с `photon_core.h`
(`photon-bindings/c`) в списке отсутствовал, а `photon_jni.c` включает этот
заголовок строкой `#include "photon_core.h"`. Проверено прямо, без JDK:
пробный файл с той же строкой включения не компилируется под `-I.` и
компилируется под `-I../../c`. Отказ громкий (`fatal error: photon_core.h: No
such file or directory`), но команда в README была нерабочей — и раздел «Что
проверено» ниже утверждает обратное.

> **Не проверено.** Команды выше исправлены по разбору и по проверке
> разрешения включений, но целиком не выполнены: в текущей среде нет ни JDK,
> ни `kotlinc` (`javac`, `java`, `kotlinc` отсутствуют). Выполнены и проверены
> только шаг 1 (сборка `.so` и генерация заголовка по имени `-o`) и
> разрешение `#include "photon_core.h"` из шага 3. Таблица «Что проверено»
> ниже относится к прогону на машине, где JDK был, — и, как показывает
> предыдущий абзац, командам из этого файла она не соответствует.

## ✅ Что проверено (реально выполнено)

| Проверка | Результат |
|---|---|
| Компиляция JNI против настоящего `jni.h` (JDK 21) | ✅ `gcc -Wall -Wextra`, чисто |
| Линковка против реальной `libphoton_core.so` | ✅ `ldd` подтверждает зависимость |
| Соответствие JNI-сигнатур | ✅ Заголовок сгенерирован `javac -h`, не написан вручную |
| Compile-time соответствие кодов `PhotonResult` Java↔C | ✅ **11** static assert'ов в `photon_jni.c` (в таблице было 10 — пересчитано) |
| Граница ABI не нарушена | ✅ `nm -D`: 0 обращений к Go-символам |
| **Java E2E на JVM** | ✅ **32/32 PASS** (с ADR-059 — **26/26**: три строки ниже, помеченные ADR-059, удалены) |
| **Kotlin E2E на JVM** | ✅ **14/14 PASS** |
| ~~Ownership: `destroy` ×3 на одном handle~~ | удалено ADR-059: повторный `destroy` — UB |
| Ownership: `destroy(0)`, `create→destroy→create` | ✅ |
| ~~Ownership: `sendAction`/`getLastError` на уничтоженном handle~~ | удалено ADR-059: вызов за окном — UB |
| Изоляция: уничтожение одного handle не влияет на другие | ✅ |
| Конкурентность (Java): 8 потоков × 500 циклов | ✅ без падений |
| Конкурентность (Kotlin): 8 потоков × 200 циклов | ✅ без падений |
| Валидация action-кодов через JNI | ✅ `INVALID_ACTION` vs `INTERNAL` различаются корректно |

## ❌ Что НЕ проверено (нет Android toolchain)

| Не проверено | Причина |
|---|---|
| Native build под `arm64-v8a` / `armeabi-v7a` / `x86_64` | Android NDK недоступен (`dl.google.com` заблокирован в среде) |
| Поведение на Android ART | Нет NDK/эмулятора. **JVM-тесты подтверждают JNI ABI и логику моста, но не ART-специфику** |
| Реальное устройство / эмулятор | Нет Android SDK |
| `AttachCurrentThread`/`DetachCurrentThread` для событий | `Photon_SetEventCallback` пока не проброшен в JNI — ядро не отправляет ни одного события. Появится вместе с фоновым коннектором (не с Dispatcher: тот реализован, Phase 13.5, а подключение делает синхронно по первому вызову). Именно тут ART-специфика критична |
| Gradle/AGP интеграция, `CMakeLists.txt` для NDK | Требует Android SDK |

**Важно:** отсутствие Android-проверки означает, что этот мост нельзя
считать «готовым для Android» — только «корректным по JNI-контракту и
логике». Первый шаг на машине с NDK: собрать под ARM и прогнать те же
два E2E-теста на устройстве/эмуляторе.

## Ограничение среды: Kotlin 1.3

`kotlinc` в песочнице — версия 1.3.31 (2019). Из-за этого:
- убрана trailing comma в `PhotonException` (появилась в Kotlin 1.4);
- `Photon` реализует `java.io.Closeable`, а не `AutoCloseable` —
  расширение `use{}` для `AutoCloseable` в 1.3 отсутствует. `Closeable`
  наследует `AutoCloseable`, поэтому на современных версиях поведение
  идентично, но при переходе на актуальный Kotlin это стоит пересмотреть.

## Заметка про `Photon_SetEventCallback`

Не проброшен в JNI сознательно. Callback требует
`AttachCurrentThread`/`DetachCurrentThread` и `GlobalRef` на объект
слушателя — и то, и другое имеет ART-специфику, которую в этой среде
проверить нельзя. Пробрасывать непроверяемый код ради галочки хуже, чем
явно отложить до момента, когда события начнут реально существовать и
когда будет доступен NDK.

Момент уточнён после Phase 13.5: **это не появление Dispatcher.** Диспетчер
реализован, а событий нет — подключение к Bridge синхронное и ленивое, по
первому `Photon_SendAction`, и уведомлять о нём нечем и незачем. Первое
событие (`CONNECTED`) придёт от фонового коннектора, который станет
обязателен вместе с push: подписку не запускает ни один вызов ABI
(`photon-core/docs/action-routing.md` §3.2).
