const axios = require("axios");
const config = require("./config");
const { ensureIndexes, getCollections } = require("./db");
const { processDetailAssets } = require("./assetSync");
const { sleep, calcDelayMs } = require("./utils");

const log = (message, meta = null) => {
    const stamp = new Date().toISOString();
    if (meta) {
        process.stdout.write(`[${stamp}] ${message} ${JSON.stringify(meta)}\n`);
        return;
    }
    process.stdout.write(`[${stamp}] ${message}\n`);
};

const buildListParams = (offset, listQueryOverride) => ({
    index: config.listIndex,
    limit: config.listLimit,
    narrow: config.listNarrow,
    offset,
    order_by: config.listOrderBy,
    page_type: config.listPageType,
    q: listQueryOverride ?? config.listQuery,
    tag: config.listTag,
    view: config.listView
});

const getListError = (meta) => {
    if (!meta || typeof meta !== "object") return null;
    if (meta.error) return meta.error;
    if (meta.error_type) return `${meta.error_type}`;
    return null;
};

const isMaxWindowError = (message) => {
    if (!message) return false;
    return (
        message.includes("max_result_window") ||
        message.includes("Result window is too large") ||
        message.includes("result window is too large")
    );
};

const buildDefaultHeaders = () => {
    const headers = {
        Accept: "application/json, text/plain, */*",
        "User-Agent": config.apiUserAgent
    };
    if (config.apiReferer) headers.Referer = config.apiReferer;
    if (config.apiCookie) headers.Cookie = config.apiCookie;
    if (config.apiCsrfToken) headers["X-Csrftoken"] = config.apiCsrfToken;
    return headers;
};

const axiosInstance = axios.create({
    timeout: config.requestTimeoutMs,
    headers: buildDefaultHeaders()
});

const requestWithRetry = async (requestFn, context) => {
    let lastError;
    for (let attempt = 1; attempt <= config.maxRetries; attempt += 1) {
        try {
            return await requestFn();
        } catch (error) {
            lastError = error;
            const status = error.response?.status;
            if (status === 429) {
                await sleep(config.rateLimitDelaySec * 1000);
            } else if (attempt < config.maxRetries) {
                await sleep(config.retryDelaySec * 1000);
            }
        }
    }
    const message = lastError?.message || "Unknown error";
    const status = lastError?.response?.status || null;
    const data = lastError?.response?.data || null;
    const { errorCollection } = await getCollections();
    await errorCollection.insertOne({
        ...context,
        status,
        message,
        data,
        headers: lastError?.response?.headers || null,
        createdAt: new Date()
    });
    throw lastError;
};

const fetchListPage = async (offset, listQueryOverride) => {
    const params = buildListParams(offset, listQueryOverride);
    const response = await axiosInstance.get(config.listBaseUrl, { params });
    return response.data;
};

const fetchDetail = async (slug) => {
    const url = `${config.detailBaseUrl}/${slug}`;
    const response = await axiosInstance.get(url, {
        params: { __env: config.detailEnv, __user: config.detailUser }
    });
    return response.data;
};

const getListPayload = (response) => {
    if (!response || !Array.isArray(response.objects) || !response.objects[0]) {
        return { items: [], meta: {} };
    }
    const [wrapper] = response.objects;
    return {
        items: wrapper.objects || [],
        meta: wrapper.meta || {}
    };
};

const buildListDoc = (item, meta) => ({
    slug: item.slug,
    problem_id: item.problem_id || item.django_id || null,
    category: item.category || null,
    status: item.status || null,
    level: item.level || null,
    modified: item.modified ? new Date(item.modified) : null,
    fetchedAt: new Date(),
    listOffset: meta.offset ?? null,
    listPageNumber: meta.page_number ?? null,
    raw: item
});

const buildDetailDoc = (slug, data, meta) => ({
    slug,
    id: data.id || null,
    category: data.category || null,
    status: data.status || null,
    level: data.level || null,
    modified: data.modified ? new Date(data.modified) : null,
    fetchedAt: new Date(),
    listOffset: meta.offset ?? null,
    listPageNumber: meta.page_number ?? null,
    raw: data
});

const updateState = async (stateCollection, updates) => {
    await stateCollection.updateOne(
        { _id: config.stateDocId },
        { $set: { updatedAt: new Date(), ...updates } },
        { upsert: true }
    );
};

const main = async () => {
    log("Sync starting.");
    await ensureIndexes();
    log("Indexes ensured.");
    const {
        listCollection,
        detailCollection,
        stateCollection,
        errorCollection
    } = await getCollections();
    log("Mongo collections ready.");

    const existingState = await stateCollection.findOne({
        _id: config.stateDocId
    });

    const queryList =
        config.listQueryList && config.listQueryList.length
            ? config.listQueryList
            : null;
    if (existingState?.status === "completed" && !config.forceResume) {
        if (!queryList || existingState?.multiQuery?.currentIndex >= queryList.length) {
            log("Sync already completed. Set FORCE_RESUME=true to run again.");
            return;
        }
    }

    let offset = existingState?.lastOffset ?? 0;
    let totalCount = existingState?.totalCount ?? null;
    let totalPages = existingState?.totalPages ?? null;
    let listRequests = existingState?.listRequests ?? 0;
    let detailRequests = existingState?.detailRequests ?? 0;
    let listItemsSaved = existingState?.listItemsSaved ?? 0;
    let detailItemsSaved = existingState?.detailItemsSaved ?? 0;
    let detailItemsSkipped = existingState?.detailItemsSkipped ?? 0;
    let failedRequests = existingState?.failedRequests ?? 0;

    await updateState(stateCollection, {
        status: "running",
        startedAt: existingState?.startedAt || new Date(),
        lastOffset: offset,
        listRequests,
        detailRequests,
        listItemsSaved,
        detailItemsSaved,
        detailItemsSkipped,
        failedRequests,
        delayMode: config.delayMode
    });
    log("State initialized.", {
        offset,
        listRequests,
        detailRequests,
        delayMode: config.delayMode
    });

    const multiQueryState =
        existingState?.multiQuery || {
            queries: queryList || [],
            currentIndex: 0,
            perQuery: {}
        };

    const persistState = async (updates) => {
        await updateState(stateCollection, {
            ...updates,
            multiQuery: multiQueryState
        });
    };

    const runQuery = async (listQueryOverride) => {
        const queryKey = listQueryOverride || "";
        const perQuery =
            multiQueryState.perQuery[queryKey] || {
                status: "running",
                lastOffset: 0,
                listRequests: 0,
                detailRequests: 0,
                listItemsSaved: 0,
                detailItemsSaved: 0,
                detailItemsSkipped: 0,
                failedRequests: 0
            };
        multiQueryState.perQuery[queryKey] = perQuery;

        let localOffset = perQuery.lastOffset ?? 0;

        while (true) {
            if (
                Number.isFinite(config.listMaxResultWindow) &&
                localOffset + config.listLimit > config.listMaxResultWindow
            ) {
                const message = `Reached max result window (${config.listMaxResultWindow}).`;
                perQuery.status = "completed";
                perQuery.lastOffset = localOffset;
                perQuery.stopReason = "max_result_window";
                perQuery.lastError = message;
                perQuery.completedAt = new Date();
                await persistState({
                    status: "running",
                    lastOffset: localOffset,
                    totalCount,
                    totalPages,
                    lastError: message,
                    stopReason: "max_result_window",
                    currentQuery: queryKey
                });
                log(message, { offset: localOffset, listLimit: config.listLimit, query: queryKey });
                break;
            }
            let listResponse;
            try {
                listRequests += 1;
                perQuery.listRequests += 1;
                await persistState({ listRequests, currentQuery: queryKey });
                listResponse = await requestWithRetry(
                    () => fetchListPage(localOffset, listQueryOverride),
                    { type: "list", offset: localOffset, url: config.listBaseUrl, query: queryKey }
                );
                log("List page fetched.", {
                    offset: localOffset,
                    query: queryKey,
                    count: listResponse?.objects?.[0]?.objects?.length || 0
                });
            } catch (error) {
                failedRequests += 1;
                perQuery.failedRequests += 1;
                perQuery.lastError = error.message;
                await persistState({
                    failedRequests,
                    lastError: error.message,
                    lastOffset: localOffset,
                    currentQuery: queryKey
                });
                throw error;
            }

            const { items, meta } = getListPayload(listResponse);
            const listError = getListError(meta);
            if (listError) {
                const stopReason = isMaxWindowError(listError)
                    ? "max_result_window"
                    : "list_error";
                perQuery.status = stopReason === "max_result_window" ? "completed" : "failed";
                perQuery.lastOffset = localOffset;
                perQuery.lastError = listError;
                perQuery.stopReason = stopReason;
                if (stopReason === "max_result_window") {
                    perQuery.completedAt = new Date();
                }
                await persistState({
                    status: stopReason === "max_result_window" ? "running" : "failed",
                    completedAt: stopReason === "max_result_window" ? new Date() : null,
                    lastOffset: localOffset,
                    totalCount,
                    totalPages,
                    lastError: listError,
                    stopReason,
                    currentQuery: queryKey
                });
                log("List error received; stopping.", {
                    offset: localOffset,
                    error: listError,
                    query: queryKey
                });
                break;
            }
            if (!items.length) {
                perQuery.status = "completed";
                perQuery.lastOffset = localOffset;
                perQuery.completedAt = new Date();
                await persistState({
                    status: "running",
                    completedAt: new Date(),
                    lastOffset: localOffset,
                    totalCount,
                    totalPages,
                    currentQuery: queryKey
                });
                log("No more list items; query completed.", { offset: localOffset, totalCount, totalPages, query: queryKey });
                break;
            }

            if (meta.total_count !== undefined) totalCount = meta.total_count;
            if (meta.total_pages !== undefined) totalPages = meta.total_pages;

            const itemsWithSlug = items.filter((item) => item.slug);
            const bulkOps = itemsWithSlug.map((item) => ({
                updateOne: {
                    filter: { slug: item.slug },
                    update: { $set: buildListDoc(item, meta) },
                    upsert: true
                }
            }));
            if (bulkOps.length > 0) {
                const bulkResult = await listCollection.bulkWrite(bulkOps, {
                    ordered: false
                });
                const upserted = bulkResult.upsertedCount || 0;
                listItemsSaved += upserted;
                perQuery.listItemsSaved += upserted;
                log("List items saved.", {
                    upserted,
                    matched: bulkResult.matchedCount || 0,
                    modified: bulkResult.modifiedCount || 0,
                    query: queryKey
                });
            }

            perQuery.lastOffset = localOffset;
            perQuery.lastPageNumber = meta.page_number ?? null;
            perQuery.lastListMeta = meta;
            perQuery.lastListFetchedAt = new Date();
            await persistState({
                lastOffset: localOffset,
                lastPageNumber: meta.page_number ?? null,
                totalCount,
                totalPages,
                listItemsSaved,
                lastListMeta: meta,
                lastListFetchedAt: new Date(),
                currentQuery: queryKey
            });

            const slugs = itemsWithSlug.map((item) => item.slug);
            let existingSlugSet = new Set();
            if (slugs.length) {
                const existing = await detailCollection
                    .find({ slug: { $in: slugs } }, { projection: { slug: 1 } })
                    .toArray();
                existingSlugSet = new Set(existing.map((doc) => doc.slug));
                log("Existing detail slugs loaded.", { count: existingSlugSet.size, query: queryKey });
            }

            for (let i = 0; i < itemsWithSlug.length; i += 1) {
                const item = itemsWithSlug[i];
                if (config.skipExistingDetails && existingSlugSet.has(item.slug)) {
                    detailItemsSkipped += 1;
                    perQuery.detailItemsSkipped += 1;
                    await persistState({
                        detailItemsSkipped,
                        lastSlugProcessed: item.slug,
                        currentQuery: queryKey
                    });
                    log("Detail skipped (already exists).", { slug: item.slug, query: queryKey });
                    continue;
                }
                if (config.detailOnlyIfMissing && existingSlugSet.has(item.slug)) {
                    detailItemsSkipped += 1;
                    perQuery.detailItemsSkipped += 1;
                    await persistState({
                        detailItemsSkipped,
                        lastSlugProcessed: item.slug,
                        currentQuery: queryKey
                    });
                    log("Detail skipped (exists; only missing enabled).", { slug: item.slug, query: queryKey });
                    continue;
                }
                if (!config.skipExistingDetails && existingSlugSet.has(item.slug)) {
                    log("Detail exists; re-fetching.", { slug: item.slug, query: queryKey });
                }

                let detailData;
                const detailStart = Date.now();
                try {
                    detailRequests += 1;
                    perQuery.detailRequests += 1;
                    await persistState({ detailRequests, currentQuery: queryKey });
                    log("Detail request starting.", { slug: item.slug, offset: localOffset, query: queryKey });
                    detailData = await requestWithRetry(
                        () => fetchDetail(item.slug),
                        {
                            type: "detail",
                            slug: item.slug,
                            offset: localOffset,
                            url: `${config.detailBaseUrl}/${item.slug}`,
                            query: queryKey
                        }
                    );
                    log("Detail fetched.", { slug: item.slug, query: queryKey });
                } catch (error) {
                    failedRequests += 1;
                    perQuery.failedRequests += 1;
                    perQuery.lastError = error.message;
                    await persistState({
                        failedRequests,
                        lastError: error.message,
                        lastSlugProcessed: item.slug,
                        lastOffset: localOffset,
                        currentQuery: queryKey
                    });
                    continue;
                }

                try {
                    log("Asset processing started.", { slug: item.slug, query: queryKey });
                    await processDetailAssets(detailData, item.slug);
                    log("Assets processed.", { slug: item.slug, query: queryKey });
                } catch (error) {
                    failedRequests += 1;
                    perQuery.failedRequests += 1;
                    perQuery.lastError = error.message;
                    await persistState({
                        failedRequests,
                        lastError: error.message,
                        lastSlugProcessed: item.slug,
                        lastOffset: localOffset,
                        currentQuery: queryKey
                    });
                    await errorCollection.insertOne({
                        type: "asset",
                        slug: item.slug,
                        offset: localOffset,
                        message: error.message,
                        createdAt: new Date()
                    });
                }

                log("Detail update starting.", { slug: item.slug, query: queryKey });
                const detailResult = await detailCollection.updateOne(
                    { slug: item.slug },
                    { $set: buildDetailDoc(item.slug, detailData, meta) },
                    { upsert: true }
                );
                detailItemsSaved += 1;
                perQuery.detailItemsSaved += 1;
                log("Detail saved.", {
                    slug: item.slug,
                    matched: detailResult.matchedCount || 0,
                    modified: detailResult.modifiedCount || 0,
                    upserted: detailResult.upsertedCount || 0,
                    durationMs: Date.now() - detailStart,
                    query: queryKey
                });

                perQuery.lastSlugProcessed = item.slug;
                perQuery.lastDetailFetchedAt = new Date();
                await persistState({
                    detailItemsSaved,
                    lastSlugProcessed: item.slug,
                    lastDetailFetchedAt: new Date(),
                    currentQuery: queryKey
                });

                const detailDelay = calcDelayMs(
                    config.delayMode,
                    config.detailDelayMinSec,
                    config.detailDelayMaxSec
                );
                if (detailDelay > 0) {
                    log("Detail delay.", { slug: item.slug, delayMs: detailDelay, query: queryKey });
                    await sleep(detailDelay);
                    log("Detail delay done.", { slug: item.slug, query: queryKey });
                }
            }

            localOffset += config.listLimit;
            perQuery.lastOffset = localOffset;
            await persistState({ lastOffset: localOffset, currentQuery: queryKey });
            log("Page completed.", { nextOffset: localOffset, query: queryKey });

            const listDelay = calcDelayMs(
                config.delayMode,
                config.listDelayMinSec,
                config.listDelayMaxSec
            );
            if (listDelay > 0) {
                log("List delay.", { delayMs: listDelay, query: queryKey });
                await sleep(listDelay);
                log("List delay done.", { delayMs: listDelay, query: queryKey });
            }
        }
    };

    if (queryList && queryList.length) {
        if (!multiQueryState.queries.length) {
            multiQueryState.queries = queryList;
        }
        for (let i = multiQueryState.currentIndex || 0; i < queryList.length; i += 1) {
            const query = queryList[i];
            multiQueryState.currentIndex = i;
            await persistState({ status: "running", currentQuery: query });
            log("Starting list query.", { query });
            await runQuery(query);
        }
        await persistState({
            status: "completed",
            completedAt: new Date(),
            currentQuery: null
        });
        log("All queries completed.");
    } else {
        await runQuery(null);
        await persistState({
            status: "completed",
            completedAt: new Date(),
            currentQuery: null
        });
        log("Sync completed.");
    }
};

main()
    .then(() => {
        log("Sync completed.");
        process.exit(0);
    })
    .catch((error) => {
        process.stderr.write(
            `[${new Date().toISOString()}] Sync failed: ${error.message}\n`
        );
        process.exit(1);
    });

