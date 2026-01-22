const path = require("path");
const config = require("./config");
const { downloadToFile, uploadToS3 } = require("./storage");

const PROJECT_BASED_TYPES = new Set(["PBT", "PBD", "PFE", "PFS"]);

const extractKeyFromUrl = (url) => {
    try {
        const parsed = new URL(url);
        return parsed.pathname.replace(/^\/+/, "");
    } catch (_error) {
        return null;
    }
};

const addAssetLog = (detailData, record) => {
    if (!detailData.extra_data) detailData.extra_data = {};
    if (!detailData.extra_data.asset_sync) {
        detailData.extra_data.asset_sync = { files: [], lastSyncedAt: null };
    }
    detailData.extra_data.asset_sync.files.push(record);
    detailData.extra_data.asset_sync.lastSyncedAt = new Date().toISOString();
};

const mirrorUrl = async ({ sourceUrl, key, slug, kind }) => {
    if (!sourceUrl || !config.assetSyncEnabled) {
        return { record: null, resultUrl: sourceUrl };
    }

    const finalKey = key || extractKeyFromUrl(sourceUrl);
    if (!finalKey) {
        return { record: null, resultUrl: sourceUrl };
    }

    const localPath = path.join(config.assetDownloadDir, finalKey);
    const record = {
        kind,
        slug,
        sourceUrl,
        key: finalKey,
        localPath,
        s3Url: null,
        status: "skipped"
    };

    const downloadedPath = await downloadToFile(sourceUrl, localPath);
    if (downloadedPath && config.s3Enabled) {
        const s3Url = await uploadToS3(finalKey, downloadedPath);
        record.s3Url = s3Url;
        record.status = s3Url ? "uploaded" : "downloaded";
        return { record, resultUrl: s3Url || sourceUrl };
    }

    record.status = downloadedPath ? "downloaded" : "skipped";
    return { record, resultUrl: sourceUrl };
};

const mirrorUrlCached = async ({ sourceUrl, key, slug, kind, cache }) => {
    if (cache && cache.has(sourceUrl)) {
        return cache.get(sourceUrl);
    }
    const result = await mirrorUrl({ sourceUrl, key, slug, kind });
    if (cache) cache.set(sourceUrl, result);
    return result;
};

const syncLocation = async (detailData, location, slug, kind, cache) => {
    if (!location || !location.s3_http_url) return;
    const sourceUrl = location.s3_http_url;
    const key = location.object_key || extractKeyFromUrl(sourceUrl);
    const { record, resultUrl } = await mirrorUrlCached({
        sourceUrl,
        key,
        slug,
        kind,
        cache
    });
    if (record) addAssetLog(detailData, record);
    if (resultUrl && resultUrl !== sourceUrl) {
        location.original_s3_http_url = sourceUrl;
        location.s3_http_url = resultUrl;
    }
};

const syncSimpleUrlField = async (detailData, container, field, slug, kind, cache) => {
    if (!container || !container[field]) return;
    const sourceUrl = container[field];
    if (typeof sourceUrl !== "string" || !sourceUrl.startsWith("http")) return;
    const key = extractKeyFromUrl(sourceUrl);
    const { record, resultUrl } = await mirrorUrlCached({
        sourceUrl,
        key,
        slug,
        kind,
        cache
    });
    if (record) addAssetLog(detailData, record);
    if (resultUrl && resultUrl !== sourceUrl) {
        container[`${field}_original`] = sourceUrl;
        container[field] = resultUrl;
    }
};

const syncDescriptionImages = async (detailData, slug, cache) => {
    if (!detailData.description) return;
    const matches = [...detailData.description.matchAll(/<img[^>]+src=["']([^"']+)["']/gi)];
    if (!matches.length) return;
    let updated = detailData.description;
    for (const match of matches) {
        const sourceUrl = match[1];
        const key = extractKeyFromUrl(sourceUrl);
        const { record, resultUrl } = await mirrorUrlCached({
            sourceUrl,
            key,
            slug,
            kind: "description_image",
            cache
        });
        if (record) addAssetLog(detailData, record);
        if (resultUrl && resultUrl !== sourceUrl) {
            updated = updated.replace(sourceUrl, resultUrl);
        }
    }
    detailData.description = updated;
};

const syncAttachments = async (detailData, slug, cache) => {
    if (!Array.isArray(detailData.attachments)) return;
    for (const attachment of detailData.attachments) {
        if (!attachment || typeof attachment !== "object") continue;
        if (attachment.s3_http_url) {
            await syncLocation(detailData, attachment, slug, "attachment", cache);
        } else if (attachment.url) {
            await syncSimpleUrlField(
                detailData,
                attachment,
                "url",
                slug,
                "attachment",
                cache
            );
        }
    }
};

const syncProjectBasedAssets = async (detailData, slug, cache) => {
    const projectData = detailData.extra_data?.project_based_problem_data;
    if (!projectData) return;
    await syncLocation(
        detailData,
        projectData.problem_solution_s3_location,
        slug,
        "problem_solution",
        cache
    );
    await syncLocation(
        detailData,
        projectData.problem_stub_s3_location,
        slug,
        "problem_stub",
        cache
    );
    await syncSimpleUrlField(
        detailData,
        detailData,
        "project_template",
        slug,
        "project_template",
        cache
    );
};

const syncUixAssets = async (detailData, slug, cache) => {
    if (!detailData.sample_solutions) return;
    await syncSimpleUrlField(
        detailData,
        detailData.sample_solutions,
        "vanillajs",
        slug,
        "sample_solution_vanillajs",
        cache
    );
    if (detailData.stubs) {
        await syncSimpleUrlField(
            detailData,
            detailData.stubs,
            "vanillajs",
            slug,
            "stub_vanillajs",
            cache
        );
    }
};

const isHttpUrl = (value) => typeof value === "string" && /^https?:\/\//i.test(value);

const extractUrls = (text) => {
    if (!isHttpUrl(text) && !text.includes("http")) return [];
    const matches = text.match(/https?:\/\/[^\s"'<>]+/gi);
    return matches ? Array.from(new Set(matches)) : [];
};

const replaceUrlsInText = async (detailData, text, slug, cache) => {
    let updated = text;
    const urls = extractUrls(text);
    for (const url of urls) {
        const key = extractKeyFromUrl(url);
        const { record, resultUrl } = await mirrorUrlCached({
            sourceUrl: url,
            key,
            slug,
            kind: "deep_scan",
            cache
        });
        if (record) addAssetLog(detailData, record);
        if (resultUrl && resultUrl !== url) {
            updated = updated.split(url).join(resultUrl);
        }
    }
    return updated;
};

const deepScanUrls = async (detailData, slug, cache) => {
    if (!config.assetScanAllUrls) return;
    const visited = new Set();

    const walk = async (node) => {
        if (!node || typeof node !== "object") return;
        if (visited.has(node)) return;
        visited.add(node);

        if (Array.isArray(node)) {
            for (let i = 0; i < node.length; i += 1) {
                const value = node[i];
                if (typeof value === "string") {
                    node[i] = await replaceUrlsInText(detailData, value, slug, cache);
                } else if (value && typeof value === "object") {
                    await walk(value);
                }
            }
            return;
        }

        for (const [key, value] of Object.entries(node)) {
            if (
                (key === "avatar_url" || key === "avatar") &&
                node &&
                (node.username || node.resource_uri || node.full_name)
            ) {
                continue;
            }
            if (typeof value === "string") {
                node[key] = await replaceUrlsInText(detailData, value, slug, cache);
            } else if (value && typeof value === "object") {
                await walk(value);
            }
        }
    };

    await walk(detailData);
};

const processDetailAssets = async (detailData, slug) => {
    if (!config.assetSyncEnabled) return;
    const cache = new Map();
    const problemType = detailData.problem_type;
    if (PROJECT_BASED_TYPES.has(problemType) || detailData.extra_data?.project_based_problem_data) {
        await syncProjectBasedAssets(detailData, slug, cache);
        await syncDescriptionImages(detailData, slug, cache);
        await syncAttachments(detailData, slug, cache);
    }
    if (problemType === "UIX") {
        await syncUixAssets(detailData, slug, cache);
        await syncDescriptionImages(detailData, slug, cache);
        await syncAttachments(detailData, slug, cache);
    }
    await deepScanUrls(detailData, slug, cache);
};

module.exports = { processDetailAssets };

