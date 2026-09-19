package dev.photon.devtools

import dev.photon.sdk.PhotonEvent

/*
 * Единственное место, где событие превращается в СТРОКУ ДЛЯ ПОКАЗА.
 *
 * Прежде здесь жил разбор `event_json` регулярным выражением — сначала в
 * четырёх копиях по лабораториям, потом сведённый сюда. После перехода на
 * SDK (ADR-062 п. 7) разбора не осталось вовсе: слушатель получает
 * типизированный [PhotonEvent], и инструменту незачем знать, как устроен
 * конверт.
 *
 * Осталась только подпись для вывода — это забота показа, а не протокола, и
 * в SDK ей не место.
 */

/**
 * Имя типа события для журнала и таблиц.
 *
 * Для [PhotonEvent.Unknown] печатается значение `type`, если ядро его
 * прислало, иначе `?`: различие «тип есть, но SDK его не знает» против
 * «типа получить не удалось» видно прямо в выводе.
 */
fun label(event: PhotonEvent): String = when (event) {
    is PhotonEvent.Connected -> "CONNECTED"
    is PhotonEvent.Message -> "MESSAGE"
    is PhotonEvent.NetworkChanged -> "NETWORK_CHANGED"
    is PhotonEvent.Unknown -> event.type ?: "?"
}
