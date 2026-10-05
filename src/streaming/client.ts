import * as grpc from "@grpc/grpc-js";
import { GrpcTransport } from "@protobuf-ts/grpc-transport";
import { RpcError } from "@protobuf-ts/runtime-rpc";
import { StreamingServiceClient } from "../generated/proto/streaming.client";
import { Status, Command } from "../generated/proto/streaming";

const RETRYABLE = new Set(["UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED"]);
const MAX_ATTEMPTS = 5;
const DELAY_BASE = 1000;
const DELAY_CALC = (attempt: number) =>
    Math.min(DELAY_BASE * 2 ** (attempt - 1), 30000) * (0.5 + Math.random());
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

let token = "Bearer abcd";

function fetchNewToken(): Promise<string> {
    return Promise.resolve("Bearer abc");
}

const transport = new GrpcTransport({
    host: "localhost:50051",
    channelCredentials: grpc.credentials.createInsecure(),
});
const client = new StreamingServiceClient(transport);

type SensorStream = ReturnType<typeof client.streamSensorReadings>;

async function receive(stream: SensorStream) {
    for await (const command of stream.responses) {
        console.log("Command from server:", command);
        if (command.command === Command.REBOOT) {
            console.log("Reboot command received from server");
        }
    }
}

async function send(stream: SensorStream) {
    for (let i = 0; i < 5; i++) {
        const reading = {
            deviceId: "sensor-001",
            temperature: 20 + i * 4,
            humidity: 40,
            status: Status.OK,
        };
        console.log("Sending:", reading);
        await stream.requests.send(reading);
        await sleep(1000);
    }
    await stream.requests.complete();
}

// One attempt: opens a fresh stream, sends and receives until the server ends it
async function runOnce(authToken: string) {
    const stream = client.streamSensorReadings({
        meta: { authorization: authToken, "x-client-id": "sensor-001" },
    });

    await Promise.all([receive(stream), send(stream)]);

    console.log("headers", await stream.headers);
    console.log("trailers", await stream.trailers);
    console.log("status", await stream.status);
}

let refreshed = false;

for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
        await runOnce(token);
        break;
    } catch (err) {
        if (!(err instanceof RpcError)) throw err;

        console.error(`gRPC error ${err.code}:`, err.message, err.meta);

        // Token expired or wrong: renew once and retry immediately
        if (err.code === "UNAUTHENTICATED" && !refreshed) {
            token = await fetchNewToken();
            refreshed = true;
            continue;
        }

        if (!RETRYABLE.has(err.code) || attempt === MAX_ATTEMPTS) {
            console.error("Giving up:", err.code, err.message);
            process.exitCode = 1;
            break;
        }

        const delay = DELAY_CALC(attempt);
        console.log(`Attempt ${attempt} failed (${err.code}), new attempt in ${Math.round(delay)} ms`);
        await sleep(delay);
    }
}
