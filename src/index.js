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

const buildListParams = (offset) => ({
    index: config.listIndex,
    limit: config.listLimit,
    narrow: config.listNarrow,
    offset,
    order_by: config.listOrderBy,
    page_type: config.listPageType,
    q: config.listQuery,
    tag: config.listTag,
    view: config.listView
});

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

const fetchListPage = async (offset) => {
    const params = buildListParams(offset);
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

    while (true) {
        let listResponse;
        try {
            listRequests += 1;
            await updateState(stateCollection, { listRequests });
            listResponse = await requestWithRetry(
                () => fetchListPage(offset),
                { type: "list", offset, url: config.listBaseUrl }
            );
            log("List page fetched.", { offset, count: listResponse?.objects?.[0]?.objects?.length || 0 });
        } catch (error) {
            failedRequests += 1;
            await updateState(stateCollection, {
                failedRequests,
                lastError: error.message,
                lastOffset: offset
            });
            throw error;
        }

        const { items, meta } = getListPayload(listResponse);
        if (!items.length) {
            await updateState(stateCollection, {
                status: "completed",
                completedAt: new Date(),
                lastOffset: offset,
                totalCount,
                totalPages
            });
            log("No more list items; sync completed.", { offset, totalCount, totalPages });
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
            listItemsSaved += bulkResult.upsertedCount || 0;
            log("List items saved.", { upserted: bulkResult.upsertedCount || 0 });
        }

        await updateState(stateCollection, {
            lastOffset: offset,
            lastPageNumber: meta.page_number ?? null,
            totalCount,
            totalPages,
            listItemsSaved,
            lastListMeta: meta,
            lastListFetchedAt: new Date()
        });

        const slugs = itemsWithSlug.map((item) => item.slug);
        let existingSlugSet = new Set();
        if (config.skipExistingDetails && slugs.length) {
            const existing = await detailCollection
                .find({ slug: { $in: slugs } }, { projection: { slug: 1 } })
                .toArray();
            existingSlugSet = new Set(existing.map((doc) => doc.slug));
            log("Existing detail slugs loaded.", { count: existingSlugSet.size });
        }

        for (let i = 0; i < itemsWithSlug.length; i += 1) {
            const item = itemsWithSlug[i];
            if (config.skipExistingDetails && existingSlugSet.has(item.slug)) {
                detailItemsSkipped += 1;
                await updateState(stateCollection, {
                    detailItemsSkipped,
                    lastSlugProcessed: item.slug
                });
                log("Detail skipped (already exists).", { slug: item.slug });
                continue;
            }

            let detailData;
            try {
                detailRequests += 1;
                await updateState(stateCollection, { detailRequests });
                detailData = await requestWithRetry(
                    () => fetchDetail(item.slug),
                    {
                        type: "detail",
                        slug: item.slug,
                        offset,
                        url: `${config.detailBaseUrl}/${item.slug}`
                    }
                );
                log("Detail fetched.", { slug: item.slug });
            } catch (error) {
                failedRequests += 1;
                await updateState(stateCollection, {
                    failedRequests,
                    lastError: error.message,
                    lastSlugProcessed: item.slug,
                    lastOffset: offset
                });
                continue;
            }

            try {
                await processDetailAssets(detailData, item.slug);
                log("Assets processed.", { slug: item.slug });
            } catch (error) {
                failedRequests += 1;
                await updateState(stateCollection, {
                    failedRequests,
                    lastError: error.message,
                    lastSlugProcessed: item.slug,
                    lastOffset: offset
                });
                await errorCollection.insertOne({
                    type: "asset",
                    slug: item.slug,
                    offset,
                    message: error.message,
                    createdAt: new Date()
                });
            }

            await detailCollection.updateOne(
                { slug: item.slug },
                { $set: buildDetailDoc(item.slug, detailData, meta) },
                { upsert: true }
            );
            detailItemsSaved += 1;
            log("Detail saved.", { slug: item.slug });

            await updateState(stateCollection, {
                detailItemsSaved,
                lastSlugProcessed: item.slug,
                lastDetailFetchedAt: new Date()
            });

            const detailDelay = calcDelayMs(
                config.delayMode,
                config.detailDelayMinSec,
                config.detailDelayMaxSec
            );
            if (detailDelay > 0) await sleep(detailDelay);
        }

        offset += config.listLimit;
        await updateState(stateCollection, { lastOffset: offset });
        log("Page completed.", { nextOffset: offset });

        const listDelay = calcDelayMs(
            config.delayMode,
            config.listDelayMinSec,
            config.listDelayMaxSec
        );
        if (listDelay > 0) {
            log("List delay.", { delayMs: listDelay });
            await sleep(listDelay);
        }
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

