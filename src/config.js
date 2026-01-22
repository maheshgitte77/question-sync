require("dotenv").config();

const parseBool = (value, fallback) => {
    if (value === undefined) return fallback;
    return ["1", "true", "yes", "y"].includes(String(value).toLowerCase());
};

const parseIntSafe = (value, fallback) => {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) ? parsed : fallback;
};

const parseFloatSafe = (value, fallback) => {
    const parsed = Number.parseFloat(value);
    return Number.isFinite(parsed) ? parsed : fallback;
};

const delayMode = (process.env.DELAY_MODE || "delayed").toLowerCase();
const appendNarrowFilter = (narrow, key, value) => {
    if (!value) return narrow;
    if (narrow.includes(`${key}|`)) return narrow;
    const trimmed = (narrow || "").trim();
    const separator = trimmed.endsWith("||") || trimmed.length === 0 ? "" : "||";
    return `${trimmed}${separator}${key}|${value}||`;
};
const s3Bucket = process.env.S3_BUCKET || "";
const s3Region = process.env.S3_REGION || "";
const s3AccessKeyId = process.env.S3_ACCESS_KEY_ID || "";
const s3SecretAccessKey = process.env.S3_SECRET_ACCESS_KEY || "";
const s3BaseUrl =
    process.env.S3_BASE_URL ||
    (s3Bucket && s3Region
        ? `https://${s3Bucket}.s3.${s3Region}.amazonaws.com`
        : "");
const s3Enabled = parseBool(
    process.env.S3_ENABLED,
    Boolean(s3Bucket && s3Region && s3AccessKeyId && s3SecretAccessKey)
);

module.exports = {
    mongoUri: process.env.MONGODB_URI || "",
    dbName: process.env.MONGODB_DB || "",
    listBaseUrl: process.env.LIST_BASE_URL || "",
    detailBaseUrl:
        process.env.DETAIL_BASE_URL || "",
    listIndex: process.env.LIST_INDEX || "problem",
    listLimit: parseIntSafe(process.env.LIST_LIMIT, 200),
    listNarrow: appendNarrowFilter(
        process.env.LIST_NARROW || "",
        "problem_type",
        process.env.LIST_PROBLEM_TYPE || ""
    ),
    listOrderBy: process.env.LIST_ORDER_BY || "-modified",
    listPageType: process.env.LIST_PAGE_TYPE || "library",
    listQuery: process.env.LIST_QUERY || "",
    listTag: process.env.LIST_TAG || "",
    listView: process.env.LIST_VIEW || "",
    detailEnv: process.env.DETAIL_ENV || "",
    detailUser: process.env.DETAIL_USER || "",
    requestTimeoutMs: parseIntSafe(process.env.REQUEST_TIMEOUT_MS, 20000),
    maxRetries: parseIntSafe(process.env.MAX_RETRIES, 3),
    retryDelaySec: parseFloatSafe(process.env.RETRY_DELAY_SEC, 5),
    rateLimitDelaySec: parseFloatSafe(process.env.RATE_LIMIT_DELAY_SEC, 60),
    skipExistingDetails: parseBool(process.env.SKIP_EXISTING_DETAILS, false),
    delayMode,
    detailDelayMinSec: parseFloatSafe(process.env.DETAIL_DELAY_MIN_SEC, 1),
    detailDelayMaxSec: parseFloatSafe(process.env.DETAIL_DELAY_MAX_SEC, 3),
    listDelayMinSec: parseFloatSafe(process.env.LIST_DELAY_MIN_SEC, 2),
    listDelayMaxSec: parseFloatSafe(process.env.LIST_DELAY_MAX_SEC, 5),
    stateDocId: process.env.STATE_DOC_ID || "",
    apiCookie: process.env.API_COOKIE || "",
    apiCsrfToken: process.env.API_CSRF_TOKEN || "",
    apiUserAgent:
        process.env.API_USER_AGENT ||
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    apiReferer: process.env.API_REFERER || "",
    assetSyncEnabled: parseBool(process.env.ASSET_SYNC_ENABLED, true),
    assetScanAllUrls: parseBool(process.env.ASSET_SCAN_ALL_URLS, true),
    assetDownloadEnabled: parseBool(process.env.ASSET_DOWNLOAD_ENABLED, true),
    assetOverwrite: parseBool(process.env.ASSET_OVERWRITE, false),
    assetDownloadDir: process.env.ASSET_DOWNLOAD_DIR || "downloads",
    assetKeyPrefix: process.env.ASSET_KEY_PREFIX || "Questions_Assets",
    s3Bucket,
    s3Region,
    s3AccessKeyId,
    s3SecretAccessKey,
    s3BaseUrl,
    s3Enabled
};

