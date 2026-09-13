package dev.photon.devtools

import com.google.protobuf.DescriptorProtos.DescriptorProto
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto
import com.google.protobuf.DescriptorProtos.FileDescriptorProto
import com.google.protobuf.DescriptorProtos.FileDescriptorSet
import java.io.File

/*
 * Protobuf Inspector — канонический каталог протокола на стороне Desktop.
 *
 * Источник истины ровно один: photon-core/protobuf/photon.proto. Из него
 * задача Gradle получает FileDescriptorSet тем же protoc, которым
 * сгенерирован photon.pb.go, и Inspector читает ЕГО. Ни таблицы действий, ни
 * enum'а, ни собственного парсера .proto здесь нет и быть не должно: любая
 * такая вещь стала бы вторым каталогом, расходящимся молча.
 *
 * Дескриптор не коммитится: он выводится из схемы, а значит не может
 * отстать от неё.
 */

/** Номера полей в дескрипторе — по ним строится путь в SourceCodeInfo. */
private const val FILE_MESSAGE_TYPE = 4
private const val FILE_ENUM_TYPE = 5
private const val ENUM_VALUE = 2
private const val MESSAGE_FIELD = 2

/**
 * Действия, которые отправляет САМ инструмент.
 *
 * Это не копия каталога, а перечень собственных вызовов: он существует, чтобы
 * места отправки и проверка контракта пользовались одним списком. Проверка
 * сверяет его с каноническим каталогом, и расхождение видно сразу.
 */
object UsedActions {
    const val CREATE_POST = 100
    const val GET_FEED = 101
    const val DELETE_POST = 102
    const val GET_ME = 200
    const val GET_OR_CREATE_DIRECT_CHANNEL = 308

    /** Заведомо отсутствующий код — им проверяется отказ пути, а не каталог. */
    const val DELIBERATELY_UNKNOWN = 999999

    val sentByTool: Map<String, Int> = linkedMapOf(
        "Dashboard: состояние субъекта" to GET_ME,
        "Dashboard: демонстрация вердикта ok:false" to GET_OR_CREATE_DIRECT_CHANNEL,
        "Network Lab: все сценарии" to GET_ME,
        "Logging: пост для проверки корреляции" to CREATE_POST,
        "Logging: уборка поста" to DELETE_POST,
        "Storage Lab: чтение страницы ленты" to GET_FEED,
        "Storage Lab: заготовка ленты и догрузка" to CREATE_POST,
    )
}

/** Комментарии схемы, доступные только при --include_source_info. */
private class Comments(file: FileDescriptorProto) {
    private val byPath: Map<List<Int>, String> =
        file.sourceCodeInfo.locationList
            .filter { it.hasLeadingComments() }
            .associate { it.pathList.toList() to it.leadingComments.trim() }

    fun at(vararg path: Int): String? = byPath[path.toList()]
    val available: Boolean = byPath.isNotEmpty()
}

class Inspector(descriptor: File) {

    private val set: FileDescriptorSet = descriptor.inputStream().use { FileDescriptorSet.parseFrom(it) }
    private val file: FileDescriptorProto = set.fileList.first()
    private val comments = Comments(file)

    val sourceName: String get() = file.name
    val packageName: String get() = file.getPackage()
    val sourceInfoAvailable: Boolean get() = comments.available

    fun enum(name: String): EnumDescriptorProto? =
        file.enumTypeList.firstOrNull { it.name == name }

    fun enumIndex(name: String): Int = file.enumTypeList.indexOfFirst { it.name == name }

    fun messages(): List<DescriptorProto> = file.messageTypeList

    /** Комментарий перед значением enum — в photon.proto это заголовки диапазонов. */
    fun enumValueComment(enumName: String, valueIndex: Int): String? =
        comments.at(FILE_ENUM_TYPE, enumIndex(enumName), ENUM_VALUE, valueIndex)

    fun enumComment(enumName: String): String? = comments.at(FILE_ENUM_TYPE, enumIndex(enumName))

    fun messageComment(index: Int): String? = comments.at(FILE_MESSAGE_TYPE, index)

    fun fieldComment(messageIndex: Int, fieldIndex: Int): String? =
        comments.at(FILE_MESSAGE_TYPE, messageIndex, MESSAGE_FIELD, fieldIndex)

    /** Проверка контракта: коды, которые шлёт инструмент, против каталога. */
    fun verifyUsedActions(): List<String> {
        val action = enum("Action") ?: return listOf("enum Action отсутствует в дескрипторе")
        val byNumber = action.valueList.associateBy { it.number }
        return UsedActions.sentByTool.map { (where, code) ->
            val v = byNumber[code]
            if (v != null) "  ✅ $code  ${v.name}  ← $where"
            else "  ❌ $code  В КАТАЛОГЕ НЕТ  ← $where"
        }
    }

    private fun typeOf(f: FieldDescriptorProto): String = when (f.type) {
        FieldDescriptorProto.Type.TYPE_MESSAGE, FieldDescriptorProto.Type.TYPE_ENUM ->
            f.typeName.removePrefix(".$packageName.")
        else -> f.type.name.removePrefix("TYPE_").lowercase()
    }

    private fun printEnum(name: String, purpose: String) {
        val e = enum(name)
        if (e == null) {
            println("\n  enum $name — в дескрипторе отсутствует")
            return
        }
        println("\n── enum $name — ${e.valueCount} значений ".padEnd(60, '─'))
        println("  назначение: $purpose")
        enumComment(name)?.let { c -> c.lines().forEach { println("  # ${it.trim()}") } }
        e.valueList.forEachIndexed { i, v ->
            enumValueComment(name, i)?.let { c ->
                c.lines().filter { it.isNotBlank() }.forEach { println("\n  ${it.trim()}") }
            }
            println("    %-6d %s".format(v.number, v.name))
        }
    }

    fun printCatalog(showMessages: Boolean, messageLimit: Int) {
        println("Protobuf Inspector — канонический каталог протокола")
        println("источник: $sourceName (package $packageName) → protoc → FileDescriptorSet")
        println("собственного каталога у инструмента нет: всё ниже прочитано из дескриптора")
        println("комментарии схемы (--include_source_info): " +
                if (sourceInfoAvailable) "доступны" else "ОТСУТСТВУЮТ")

        printEnum("Resource", "адресуемые ресурсы протокола")
        printEnum("Action", "коды действий Photon_SendAction")

        // Два РАЗНЫХ словаря, и смешивать их нельзя.
        printEnum("EventType", "ДОМЕННЫЕ события протокола (Bridge). " +
                "НЕ типы событий подписки C ABI — см. врезку ниже")

        println("\n── события подписки C ABI — другой словарь ".padEnd(60, '─'))
        println("  В event_json от Photon_SetEventCallback приходят CONNECTED,")
        println("  MESSAGE и NETWORK_CHANGED. В photon.proto этих имён нет вовсе:")
        println("  их источник — photon_core.h и ADR-045, а enum EventType выше —")
        println("  доменные события Bridge. Inspector их НЕ перечисляет: список,")
        println("  вписанный сюда руками, был бы второй копией истины.")

        println("\n── сверка контракта с тем, что шлёт этот клиент ".padEnd(60, '─'))
        verifyUsedActions().forEach { println(it) }

        if (showMessages) {
            val msgs = messages()
            println("\n── сообщения: ${msgs.size} ".padEnd(60, '─'))
            msgs.take(messageLimit).forEachIndexed { mi, m ->
                println("\n  message ${m.name} — ${m.fieldCount} полей")
                messageComment(mi)?.lines()?.firstOrNull()?.let { println("    # ${it.trim()}") }
                m.fieldList.forEachIndexed { fi, f ->
                    val label = f.label.name.removePrefix("LABEL_").lowercase()
                    val lbl = if (label == "repeated") "repeated " else ""
                    println("      %-4d %s%-24s %s".format(f.number, lbl, typeOf(f), f.name))
                    fieldComment(mi, fi)?.lines()?.firstOrNull()?.let { println("           # ${it.trim()}") }
                }
            }
            if (msgs.size > messageLimit) {
                println("\n  … ещё ${msgs.size - messageLimit}; полный список: -all")
            }
        }
    }

    companion object {
        /**
         * Путь передаёт сборка через -Dphoton.descriptor. Угадывать
         * расположение сгенерированного артефакта инструмент не должен.
         */
        fun fromSystemProperty(): Inspector {
            val path = System.getProperty("photon.descriptor")
                ?: error("не задан -Dphoton.descriptor; дескриптор генерирует задача " +
                         "Gradle generateDescriptor, запускать через ./gradlew run")
            val f = File(path)
            if (!f.isFile) {
                error("дескриптор не найден: $path — выполните: gradle descriptor")
            }
            return Inspector(f)
        }
    }
}
