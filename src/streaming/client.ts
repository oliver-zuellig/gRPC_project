import * as grpc from "@grpc/grpc-js";
import { GrpcTransport } from "@protobuf-ts/grpc-transport";
import { RpcError } from "@protobuf-ts/runtime-rpc";
import { StreamingServiceClient } from "../generated/proto/streaming.client";
import { Status, Command } from "../generated/proto/streaming";
import { service as healthService } from "grpc-health-check";

const HOST = "localhost:50051";

const RETRYABLE = new Set(["UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED"]);
const MAX_ATTEMPTS = 5;
const DELAY_BASE = 1000;
const DELAY_CALC = (attempt: number) =>
    Math.min(DELAY_BASE * 2 ** (attempt - 1), 30000) * (0.5 + Math.random());
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const TIMEOUT = 30000;

let token = "Bearer abcd";

function fetchNewToken(): Promise<string> {
    return Promise.resolve("Bearer abc");
}

const transport = new GrpcTransport({
    host: HOST,
    channelCredentials: grpc.credentials.createInsecure(),
    timeout: TIMEOUT
});
const client = new StreamingServiceClient(transport);

// Plain grpc-js client for the standard health service
const HealthClient = grpc.makeClientConstructor(healthService, "Health");
const healthClient = new HealthClient(HOST, grpc.credentials.createInsecure()) as unknown as grpc.Client & {
    check(
        request: { service: string },
        options: grpc.CallOptions,
        callback: (err: grpc.ServiceError | null, res?: { status: string | number }) => void,
    ): void;
};

// Throws an UNAVAILABLE RpcError if the server is down or not SERVING
function checkHealth(serviceName = "streaming.StreamingService"): Promise<void> {
    return new Promise((resolve, reject) => {
        healthClient.check(
            { service: serviceName },
            { deadline: Date.now() + 2000 },
            (err, res) => {
                if (err) {
                    return reject(new RpcError(`Health check failed: ${err.details}`, "UNAVAILABLE"));
                }
                if (res?.status !== "SERVING" && res?.status !== 1) {
                    return reject(new RpcError(`Service not serving (${res?.status})`, "UNAVAILABLE"));
                }
                resolve();
            },
        );
    });
}

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
    // No connection / not healthy: fail here, before anything is sent
    await checkHealth();

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
