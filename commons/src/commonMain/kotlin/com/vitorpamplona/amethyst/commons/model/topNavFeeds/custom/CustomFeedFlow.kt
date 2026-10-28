/*
 * Copyright (c) 2025 Vitor Pamplona
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
 * AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.vitorpamplona.amethyst.commons.model.topNavFeeds.custom

import androidx.compose.runtime.Immutable
import com.vitorpamplona.amethyst.commons.feeds.custom.FeedDefinition
import com.vitorpamplona.amethyst.commons.feeds.custom.FeedSource
import com.vitorpamplona.amethyst.commons.model.topNavFeeds.IFeedFlowsType
import com.vitorpamplona.amethyst.commons.model.topNavFeeds.IFeedTopNavFilter
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map

/**
 * Whether a web-of-trust limit can be applied, and the index file's generation so a feed re-filters
 * when that index is replaced. The generation is not the account's minimum score: a feed carries
 * its own.
 */
@Immutable
data class WebOfTrustFeedGate(
    val active: Boolean,
    val generation: Long,
) {
    companion object {
        val INACTIVE = WebOfTrustFeedGate(active = false, generation = 0L)
    }
}

/** Follows the custom feed [id] in [customFeeds], so editing the feed re-runs it and deleting it empties it. */
class CustomFeedFlow(
    val id: String,
    val customFeeds: StateFlow<List<FeedDefinition>>,
    val followsRelays: StateFlow<Set<NormalizedRelayUrl>>,
    val blockedRelays: StateFlow<Set<NormalizedRelayUrl>>,
    val proxyRelays: StateFlow<Set<NormalizedRelayUrl>>,
    val wotGate: StateFlow<WebOfTrustFeedGate> = MutableStateFlow(WebOfTrustFeedGate.INACTIVE),
    val wotRankOf: (HexKey) -> Int? = { null },
) : IFeedFlowsType {
    private fun sourceIn(feeds: List<FeedDefinition>) = feeds.firstOrNull { it.id == id }?.source as? FeedSource.Filter

    private fun buildFilter(
        source: FeedSource.Filter?,
        proxy: Set<NormalizedRelayUrl>,
        gate: WebOfTrustFeedGate,
    ) = CustomFeedTopNavFilter(
        source,
        followsRelays,
        blockedRelays,
        proxy,
        wotActive = gate.active,
        wotRankOf = wotRankOf,
    )

    override fun flow(): Flow<IFeedTopNavFilter> =
        combine(customFeeds.map(::sourceIn).distinctUntilChanged(), proxyRelays, wotGate) { source, proxy, gate ->
            buildFilter(source, proxy, gate)
        }

    override fun startValue(): IFeedTopNavFilter = buildFilter(sourceIn(customFeeds.value), proxyRelays.value, wotGate.value)

    override suspend fun startValue(collector: FlowCollector<IFeedTopNavFilter>) {
        collector.emit(startValue())
    }
}
