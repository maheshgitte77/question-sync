const fs = require("fs");
const path = require("path");
const axios = require("axios");
const mime = require("mime-types");
const { S3Client, PutObjectCommand, HeadObjectCommand } = require("@aws-sdk/client-s3");
const config = require("./config");

let s3Client;

const getS3Client = () => {
    if (!config.s3Enabled) return null;
    if (s3Client) return s3Client;
    s3Client = new S3Client({
        region: config.s3Region,
        credentials: {
            accessKeyId: config.s3AccessKeyId,
            secretAccessKey: config.s3SecretAccessKey
        }
    });
    return s3Client;
};

const ensureDir = async (filePath) => {
    await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
};

const downloadToFile = async (url, destPath) => {
    if (!config.assetDownloadEnabled) return null;
    if (!config.assetOverwrite && fs.existsSync(destPath)) {
        return destPath;
    }
    const response = await axios.get(url, {
        responseType: "arraybuffer",
        timeout: config.requestTimeoutMs
    });
    await ensureDir(destPath);
    await fs.promises.writeFile(destPath, Buffer.from(response.data));
    return destPath;
};

const s3ObjectExists = async (key) => {
    if (!config.s3Enabled) return false;
    const client = getS3Client();
    try {
        await client.send(
            new HeadObjectCommand({
                Bucket: config.s3Bucket,
                Key: key
            })
        );
        return true;
    } catch (_error) {
        return false;
    }
};

const uploadToS3 = async (key, filePath) => {
    if (!config.s3Enabled) return null;
    const client = getS3Client();
    if (!config.assetOverwrite) {
        const exists = await s3ObjectExists(key);
        if (exists) return `${config.s3BaseUrl}/${key}`;
    }
    const contentType =
        mime.lookup(filePath) || "application/octet-stream";
    await client.send(
        new PutObjectCommand({
            Bucket: config.s3Bucket,
            Key: key,
            Body: fs.createReadStream(filePath),
            ContentType: contentType
        })
    );
    return `${config.s3BaseUrl}/${key}`;
};

module.exports = {
    downloadToFile,
    uploadToS3
};

