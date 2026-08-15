# photon-bindings/android

JNI-мост к Photon C ABI v1 + Kotlin-обёртка.

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

```sh
# 1. Собрать C ABI библиотеку
cd photon-bindings/c
go build -buildmode=c-shared -o libphoton_core.so .

# 2. Сгенерировать JNI-заголовок
cd photon-bindings/android/java
javac -h ../jni -d /tmp/out dev/photon/core/PhotonNative.java

# 3. Собрать JNI-мост
cd ../jni
JAVA_INC=$JAVA_HOME/include
gcc -shared -fPIC -I"$JAVA_INC" -I"$JAVA_INC/linux" -I. \
    -o libphoton_jni.so photon_jni.c \
    -L../../c -l:libphoton_core.so -Wl,-rpath,../../c -Wall -Wextra

# 4. Тесты
javac -d /tmp/jnitest ../java/PhotonJniTest.java ../java/dev/photon/core/PhotonNative.java
java -cp /tmp/jnitest PhotonJniTest ./libphoton_jni.so

kotlinc ../kotlin/Photon.kt ../kotlin/PhotonKotlinTest.kt \
        ../java/dev/photon/core/PhotonNative.java -include-runtime -d /tmp/kt.jar
java -cp /tmp/kt.jar:/tmp/jnitest PhotonKotlinTestKt ./libphoton_jni.so
```

## ✅ Что проверено (реально выполнено)

| Проверка | Результат |
|---|---|
| Компиляция JNI против настоящего `jni.h` (JDK 21) | ✅ `gcc -Wall -Wextra`, чисто |
| Линковка против реальной `libphoton_core.so` | ✅ `ldd` подтверждает зависимость |
| Соответствие JNI-сигнатур | ✅ Заголовок сгенерирован `javac -h`, не написан вручную |
| Compile-time соответствие кодов `PhotonResult` Java↔C | ✅ 10 static assert'ов в `photon_jni.c` |
| Граница ABI не нарушена | ✅ `nm -D`: 0 обращений к Go-символам |
| **Java E2E на JVM** | ✅ **32/32 PASS** |
| **Kotlin E2E на JVM** | ✅ **14/14 PASS** |
| Ownership: `destroy` ×3 на одном handle | ✅ без падения (проверка double-free guard) |
| Ownership: `destroy(0)`, `create→destroy→create` | ✅ |
| Ownership: `sendAction`/`getLastError` на уничтоженном handle | ✅ `INVALID_ARGUMENT`, без падения |
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
| `AttachCurrentThread`/`DetachCurrentThread` для событий | `Photon_SetEventCallback` пока не проброшен в JNI — фоновых событий ещё нет (Dispatcher — Phase 13). Появится вместе с ними, и именно тут ART-специфика критична |
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
явно отложить до появления Dispatcher (Phase 13), когда события начнут
реально существовать и когда будет доступен NDK.
