import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import * as grpc from "@grpc/grpc-js";
import { GrpcTransport } from "@protobuf-ts/grpc-transport";
import { RpcError } from "@protobuf-ts/runtime-rpc";
import { StreamingServiceClient } from "../generated/proto/streaming.client";
import { Status, Command } from "../generated/proto/streaming";
import type { ServerCommand } from "../generated/proto/streaming";
import { createServer, listen, VALID_TOKEN } from "./server";

const reading = (temperature: number) => ({
    deviceId: "sensor-001",
    temperature,
    humidity: 40,
    status: Status.OK,
});

describe("server", () => {
    const { server, health } = createServer();
    let transport: GrpcTransport;
    let client: StreamingServiceClient;

    before(async () => {
        const port = await listen(server, "127.0.0.1:0");
        transport = new GrpcTransport({
            host: `127.0.0.1:${port}`,
            channelCredentials: grpc.credentials.createInsecure(),
        });
        client = new StreamingServiceClient(transport);
    });

    after(() => {
        transport.close();
        server.forceShutdown();
    });

    async function collect(token: string | undefined, temperatures: number[]) {
        const stream = client.streamSensorReadings({
            meta: token === undefined ? {} : { authorization: token },
        });
        const received: ServerCommand[] = [];
        const receiving = (async () => {
            for await (const command of stream.responses) received.push(command);
        })();
        for (const t of temperatures) await stream.requests.send(reading(t));
        await stream.requests.complete();
        await receiving;
        return received;
    }

    it("sends no command for readings up to 30 degrees", async () => {
        assert.deepEqual(await collect(VALID_TOKEN, [20, 25, 30]), []);
    });

    it("sends a REBOOT command for each reading above 30 degrees", async () => {
        const received = await collect(VALID_TOKEN, [20, 31, 40]);
        assert.equal(received.length, 2);
        assert.ok(received.every((c) => c.command === Command.REBOOT));
    });

    it("ends the stream cleanly when the client completes", async () => {
        const stream = client.streamSensorReadings({ meta: { authorization: VALID_TOKEN } });
        await stream.requests.send(reading(20));
        await stream.requests.complete();
        assert.equal((await stream.status).code, "OK");
    });

    it("rejects a wrong token with UNAUTHENTICATED", async () => {
        await assert.rejects(collect("wrong", [20]), (err) => {
            assert.ok(err instanceof RpcError);
            assert.equal(err.code, "UNAUTHENTICATED");
            return true;
        });
    });

    it("rejects a missing token with UNAUTHENTICATED", async () => {
        await assert.rejects(collect(undefined, [20]), (err) => {
            assert.ok(err instanceof RpcError);
            assert.equal(err.code, "UNAUTHENTICATED");
            return true;
        });
    });

    it("answers health checks and reflects status changes", async () => {
        const { service } = await import("grpc-health-check");
        const Health = grpc.makeClientConstructor(service, "Health");
        const address = transport["defaultOptions"].host as string;
        const healthClient = new Health(address, grpc.credentials.createInsecure()) as any;
        const check = (name: string) =>
            new Promise<string | number>((resolve, reject) =>
                healthClient.check({ service: name }, (err: Error | null, res: any) =>
                    err ? reject(err) : resolve(res.status),
                ),
            );
        const isServing = (s: string | number) => s === "SERVING" || s === 1;

        assert.ok(isServing(await check("streaming.StreamingService")));

        health.setStatus("streaming.StreamingService", "NOT_SERVING");
        assert.ok(!isServing(await check("streaming.StreamingService")));

        health.setStatus("streaming.StreamingService", "SERVING");
        healthClient.close();
    });
});
