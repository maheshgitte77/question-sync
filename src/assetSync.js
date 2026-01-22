const path = require("path");
const crypto = require("crypto");
const config = require("./config");
const { downloadToFile, uploadToS3 } = require("./storage");

const PROJECT_BASED_TYPES = new Set(["PBT", "PBD", "PFE", "PFS"]);

const log = (message, meta = null) => {
    const stamp = new Date().toISOString();
    if (meta) {
        process.stdout.write(`[${stamp}] ${message} ${JSON.stringify(meta)}\n`);
        return;
    }
    process.stdout.write(`[${stamp}] ${message}\n`);
};

const normalizeUrl = (url) => {
    if (!url) return url;
    try {
        return new URL(url).toString();
    } catch (_error) {
        try {
            return new URL(encodeURI(url)).toString();
        } catch (_innerError) {
            return url;
        }
    }
};

const extractKeyFromUrl = (url) => {
    try {
        const parsed = new URL(normalizeUrl(url));
        return parsed.pathname.replace(/^\/+/, "");
    } catch (_error) {
        return null;
    }
};

const getExtensionFromUrl = (url) => {
    try {
        const parsed = new URL(normalizeUrl(url));
        const ext = path.extname(parsed.pathname);
        return ext || "";
    } catch (_error) {
        return "";
    }
};

const buildNewKey = (url, slug, problemType) => {
    const ext = getExtensionFromUrl(url);
    const id = crypto.randomUUID();
    const prefix = config.assetKeyPrefix || "assets";
    const safeSlug = slug || "unknown";
    const safeType = problemType || "unknown";
    return `${prefix}/${safeType}/${safeSlug}/${id}${ext}`;
};

const addAssetLog = (detailData, record) => {
    if (!detailData.extra_data) detailData.extra_data = {};
    if (!detailData.extra_data.asset_sync) {
        detailData.extra_data.asset_sync = { files: [], lastSyncedAt: null };
    }
    detailData.extra_data.asset_sync.files.push(record);
    detailData.extra_data.asset_sync.lastSyncedAt = new Date().toISOString();
};

const mirrorUrl = async ({ sourceUrl, key, slug, kind, problemType }) => {
    if (!sourceUrl || !config.assetSyncEnabled) {
        return { record: null, resultUrl: sourceUrl };
    }

    const normalizedUrl = normalizeUrl(sourceUrl);
    if (config.s3BaseUrl && normalizedUrl.startsWith(config.s3BaseUrl)) {
        return { record: null, resultUrl: sourceUrl };
    }
    const finalKey = buildNewKey(sourceUrl, slug, problemType);
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
        status: "skipped",
        errorMessage: null,
        errorStatus: null
    };

    let downloadedPath = null;
    try {
        // log("Asset download starting.", { slug, kind, sourceUrl, key: finalKey });
        downloadedPath = await downloadToFile(normalizedUrl, localPath);
    } catch (error) {
        record.status = "failed";
        record.errorStatus = error?.response?.status || null;
        record.errorMessage = error?.message || "Asset download failed";
        // log("Asset download failed.", {
        //     slug,
        //     kind,
        //     sourceUrl,
        //     key: finalKey,
        //     status: record.errorStatus,
        //     message: record.errorMessage
        // });
        return { record, resultUrl: sourceUrl };
    }

    if (downloadedPath && config.s3Enabled) {
        try {
            // log("Asset upload starting.", { slug, kind, sourceUrl, key: finalKey });
            const s3Url = await uploadToS3(finalKey, downloadedPath);
            record.s3Url = s3Url;
            record.status = s3Url ? "uploaded" : "downloaded";
            // log("Asset upload done.", { slug, kind, sourceUrl, key: finalKey, status: record.status });
            return { record, resultUrl: s3Url || sourceUrl };
        } catch (error) {
            record.status = "failed";
            record.errorStatus = error?.$metadata?.httpStatusCode || null;
            record.errorMessage = error?.message || "Asset upload failed";
            // log("Asset upload failed.", {
            //     slug,
            //     kind,
            //     sourceUrl,
            //     key: finalKey,
            //     status: record.errorStatus,
            //     message: record.errorMessage
            // });
            return { record, resultUrl: sourceUrl };
        }
    }

    record.status = downloadedPath ? "downloaded" : "skipped";
    // log("Asset download done (no upload).", { slug, kind, sourceUrl, key: finalKey, status: record.status });
    return { record, resultUrl: sourceUrl };
};

const mirrorUrlCached = async ({ sourceUrl, key, slug, kind, cache, problemType }) => {
    if (cache && cache.has(sourceUrl)) {
        return cache.get(sourceUrl);
    }
    const result = await mirrorUrl({ sourceUrl, key, slug, kind, problemType });
    if (cache) cache.set(sourceUrl, result);
    return result;
};

const syncLocation = async (detailData, location, slug, kind, cache, problemType) => {
    if (!location || !location.s3_http_url) return;
    const sourceUrl = location.s3_http_url;
    const key = location.object_key || extractKeyFromUrl(sourceUrl);
    const { record, resultUrl } = await mirrorUrlCached({
        sourceUrl,
        key,
        slug,
        kind,
        cache,
        problemType
    });
    if (record) addAssetLog(detailData, record);
    if (resultUrl && resultUrl !== sourceUrl) {
        location.original_s3_http_url = sourceUrl;
        location.s3_http_url = resultUrl;
    }
};

const syncSimpleUrlField = async (
    detailData,
    container,
    field,
    slug,
    kind,
    cache,
    problemType
) => {
    if (!container || !container[field]) return;
    const sourceUrl = container[field];
    if (typeof sourceUrl !== "string" || !sourceUrl.startsWith("http")) return;
    const key = extractKeyFromUrl(sourceUrl);
    const { record, resultUrl } = await mirrorUrlCached({
        sourceUrl,
        key,
        slug,
        kind,
        cache,
        problemType
    });
    if (record) addAssetLog(detailData, record);
    if (resultUrl && resultUrl !== sourceUrl) {
        container[`${field}_original`] = sourceUrl;
        container[field] = resultUrl;
    }
};

const syncDescriptionImages = async (detailData, slug, cache, problemType) => {
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
            cache,
            problemType
        });
        if (record) addAssetLog(detailData, record);
        if (resultUrl && resultUrl !== sourceUrl) {
            updated = updated.replace(sourceUrl, resultUrl);
        }
    }
    detailData.description = updated;
};

const syncAttachments = async (detailData, slug, cache, problemType) => {
    if (!Array.isArray(detailData.attachments)) return;
    for (const attachment of detailData.attachments) {
        if (!attachment || typeof attachment !== "object") continue;
        if (attachment.s3_http_url) {
            await syncLocation(detailData, attachment, slug, "attachment", cache, problemType);
        } else if (attachment.url) {
            await syncSimpleUrlField(
                detailData,
                attachment,
                "url",
                slug,
                "attachment",
                cache,
                problemType
            );
        }
    }
};

const syncPrivateAttachments = async (detailData, slug, cache, problemType) => {
    if (!Array.isArray(detailData.private_attachments)) return;
    for (let i = 0; i < detailData.private_attachments.length; i += 1) {
        const sourceUrl = detailData.private_attachments[i];
        if (typeof sourceUrl !== "string" || !sourceUrl.startsWith("http")) continue;
        const key = extractKeyFromUrl(sourceUrl);
        const { record, resultUrl } = await mirrorUrlCached({
            sourceUrl,
            key,
            slug,
            kind: "private_attachment",
            cache,
            problemType
        });
        if (record) addAssetLog(detailData, record);
        if (resultUrl && resultUrl !== sourceUrl) {
            detailData.private_attachments[i] = resultUrl;
        }
    }
};

const syncProjectBasedAssets = async (detailData, slug, cache, problemType) => {
    const projectData = detailData.extra_data?.project_based_problem_data;
    if (!projectData) return;
    await syncLocation(
        detailData,
        projectData.problem_solution_s3_location,
        slug,
        "problem_solution",
        cache,
        problemType
    );
    await syncLocation(
        detailData,
        projectData.problem_stub_s3_location,
        slug,
        "problem_stub",
        cache,
        problemType
    );
    await syncSimpleUrlField(
        detailData,
        detailData,
        "project_template",
        slug,
        "project_template",
        cache,
        problemType
    );
};

const syncUixAssets = async (detailData, slug, cache, problemType) => {
    if (!detailData.sample_solutions) return;
    await syncSimpleUrlField(
        detailData,
        detailData.sample_solutions,
        "vanillajs",
        slug,
        "sample_solution_vanillajs",
        cache,
        problemType
    );
    if (detailData.stubs) {
        await syncSimpleUrlField(
            detailData,
            detailData.stubs,
            "vanillajs",
            slug,
            "stub_vanillajs",
            cache,
            problemType
        );
    }
};

const isHttpUrl = (value) => typeof value === "string" && /^https?:\/\//i.test(value);

const decodeHtmlEntities = (value) => {
    if (!value || typeof value !== "string") return value;
    return value
        .replace(/&quot;/g, "\"")
        .replace(/&#34;/g, "\"")
        .replace(/&amp;/g, "&");
};

const sanitizeUrlCandidate = (value) => {
    if (!value || typeof value !== "string") return null;
    let candidate = decodeHtmlEntities(value.trim());
    candidate = candidate.replace(/[)"'\]}>,;]+$/g, "");
    candidate = candidate.replace(/["']+$/g, "");
    if (!isHttpUrl(candidate)) return null;
    return candidate;
};

const extractUrls = (text) => {
    if (!isHttpUrl(text) && !text.includes("http")) return [];
    const matches = text.match(/https?:\/\/[^\s"'<>]+/gi);
    if (!matches) return [];
    const cleaned = matches
        .map((url) => sanitizeUrlCandidate(url))
        .filter((url) => Boolean(url));
    return Array.from(new Set(cleaned));
};

const replaceUrlsInText = async (detailData, text, slug, cache, problemType) => {
    let updated = text;
    const urls = extractUrls(text);
    for (const url of urls) {
        const key = extractKeyFromUrl(url);
        const { record, resultUrl } = await mirrorUrlCached({
            sourceUrl: url,
            key,
            slug,
            kind: "deep_scan",
            cache,
            problemType
        });
        if (record) addAssetLog(detailData, record);
        if (resultUrl && resultUrl !== url) {
            updated = updated.split(url).join(resultUrl);
        }
    }
    return updated;
};

const deepScanUrls = async (detailData, slug, cache, problemType) => {
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
                    node[i] = await replaceUrlsInText(
                        detailData,
                        value,
                        slug,
                        cache,
                        problemType
                    );
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
                node[key] = await replaceUrlsInText(
                    detailData,
                    value,
                    slug,
                    cache,
                    problemType
                );
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
        await syncProjectBasedAssets(detailData, slug, cache, problemType);
        await syncDescriptionImages(detailData, slug, cache, problemType);
        await syncAttachments(detailData, slug, cache, problemType);
    }
    if (problemType === "UIX") {
        await syncUixAssets(detailData, slug, cache, problemType);
        await syncDescriptionImages(detailData, slug, cache, problemType);
        await syncAttachments(detailData, slug, cache, problemType);
    }
    await syncPrivateAttachments(detailData, slug, cache, problemType);
    await deepScanUrls(detailData, slug, cache, problemType);
};

module.exports = { processDetailAssets };

