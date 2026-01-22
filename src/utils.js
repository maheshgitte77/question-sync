const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const randomBetween = (min, max) => {
    if (max <= min) return min;
    return min + Math.random() * (max - min);
};

const calcDelayMs = (mode, minSec, maxSec) => {
    if (mode === "immediate") return 0;
    const delaySec = randomBetween(minSec, maxSec);
    return Math.round(delaySec * 1000);
};

module.exports = { sleep, randomBetween, calcDelayMs };

