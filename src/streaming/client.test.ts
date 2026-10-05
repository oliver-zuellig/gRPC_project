import { describe, it, afterEach } from "node:test";
import assert from "node:assert/strict";
import * as grpc from "@grpc/grpc-js";
import { RpcError } from "@protobuf-ts/runtime-rpc";
import { Command } from "../generated/proto/streaming";
import { createSensorClient } from "./client";
import { createServer, listen, VALID_TOKEN } from "./server";

const silent = () => {};

// Fast settings so tests do not wait for real backoff/send intervals
const fast = { delayBase: 10, sendInterval: 5, log: silent };

describe("client", () => {
    const cleanups: Array<() => void> = [];
    afterEach(() => {
        while (cleanups.length) cleanups.pop()!();
    });

    async function startServer() {
        const { server, health } = createServer();
        const port = await listen(server, "127.0.0.1:0");
        cleanups.push(() => server.forceShutdown());
        return { server, health, host: `127.0.0.1:${port}` };
    }

    function makeClient(options: Parameters<typeof createSensorClient>[0]) {
        const sensor = createSensorClient(options);
        cleanups.push(() => sensor.close());
        return sensor;
    }

    it("receives REBOOT commands from the server", async () => {
        const { host } = await startServer();
        const sensor = makeClient({ ...fast, host, token: VALID_TOKEN, readings: 5 });

        const commands = await sensor.runWithRetry();

        // readings are 20, 24, 28, 32, 36 -> the last two are above 30
        assert.equal(commands.length, 2);
        assert.ok(commands.every((c) => c.command === Command.REBOOT));
    });

    it("fails the health check and sends nothing when the server is down", async () => {
        const { server, host } = await startServer();
        server.forceShutdown();
        const logs: unknown[][] = [];
        const sensor = makeClient({ ...fast, host, token: VALID_TOKEN, log: (...a) => logs.push(a) });

        await assert.rejects(sensor.runOnce(VALID_TOKEN), (err) => {
            assert.ok(err instanceof RpcError);
            assert.equal(err.code, "UNAVAILABLE");
            return true;
        });
        assert.ok(!logs.some((l) => l[0] === "Sending:"), "must not send while unreachable");
    });

    it("fails the health check when the service is NOT_SERVING", async () => {
        const { health, host } = await startServer();
        health.setStatus("streaming.StreamingService", "NOT_SERVING");
        const sensor = makeClient({ ...fast, host, token: VALID_TOKEN });

        await assert.rejects(sensor.checkHealth(), (err) => {
            assert.ok(err instanceof RpcError);
            assert.equal(err.code, "UNAVAILABLE");
            return true;
        });
    });

    it("gives up after maxAttempts when the server stays unreachable", async () => {
        const { server, host } = await startServer();
        server.forceShutdown();
        const logs: unknown[][] = [];
        const sensor = makeClient({
            ...fast,
            host,
            token: VALID_TOKEN,
            maxAttempts: 3,
            log: (...a) => logs.push(a),
        });

        await assert.rejects(sensor.runWithRetry(), (err) => err instanceof RpcError);
        const retries = logs.filter((l) => String(l[0]).startsWith("Attempt"));
        assert.equal(retries.length, 2); // attempts 1 and 2 retry, attempt 3 gives up
    });

    it("recovers when the server comes up between attempts", async () => {
        const { server, host } = await startServer();
        server.forceShutdown();
        const port = Number(host.split(":")[1]);

        const sensor = makeClient({ ...fast, host, token: VALID_TOKEN, delayBase: 200, readings: 1 });
        const running = sensor.runWithRetry();

        // bring a new server up on the same port while the client is backing off
        await new Promise((r) => setTimeout(r, 50));
        const { server: second } = createServer();
        cleanups.push(() => second.forceShutdown());
        await listen(second, `127.0.0.1:${port}`);

        await running;
    });

    it("renews the token once on UNAUTHENTICATED and then succeeds", async () => {
        const { host } = await startServer();
        let fetched = 0;
        const sensor = makeClient({
            ...fast,
            host,
            token: "wrong",
            readings: 1,
            fetchNewToken: async () => {
                fetched++;
                return VALID_TOKEN;
            },
        });

        await sensor.runWithRetry();
        assert.equal(fetched, 1);
    });

    it("does not retry UNAUTHENTICATED more than once", async () => {
        const { host } = await startServer();
        let fetched = 0;
        const sensor = makeClient({
            ...fast,
            host,
            token: "wrong",
            readings: 1,
            fetchNewToken: async () => {
                fetched++;
                return "still-wrong";
            },
        });

        await assert.rejects(sensor.runWithRetry(), (err) => {
            assert.ok(err instanceof RpcError);
            assert.equal(err.code, "UNAUTHENTICATED");
            return true;
        });
        assert.equal(fetched, 1);
    });
});
