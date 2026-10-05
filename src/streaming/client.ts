import * as grpc from "@grpc/grpc-js";
import { GrpcTransport } from "@protobuf-ts/grpc-transport";
import { RpcError } from "@protobuf-ts/runtime-rpc";
import { StreamingServiceClient } from "../generated/proto/streaming.client";
import { Status, Command } from "../generated/proto/streaming";
import type { ServerCommand } from "../generated/proto/streaming";
import { service as healthService } from "grpc-health-check";

const RETRYABLE = new Set(["UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED"]);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export interface ClientOptions {
    host: string;
    token: string;
    fetchNewToken?: () => Promise<string>;
    maxAttempts?: number;
    delayBase?: number;       // ms, first backoff step
    sendInterval?: number;    // ms between readings
    readings?: number;        // number of readings per attempt
    timeout?: number;         // ms, per call
    log?: (...args: unknown[]) => void;
}

export function createSensorClient(options: ClientOptions) {
    const {
        host,
        fetchNewToken = () => Promise.resolve(options.token),
        maxAttempts = 5,
        delayBase = 1000,
        sendInterval = 1000,
        readings = 5,
        timeout = 30000,
        log = console.log,
    } = options;
    let token = options.token;

    const transport = new GrpcTransport({
        host,
        channelCredentials: grpc.credentials.createInsecure(),
        timeout,
    });
    const client = new StreamingServiceClient(transport);
    type SensorStream = ReturnType<typeof client.streamSensorReadings>;

    // Plain grpc-js client for the standard health service
    const HealthClient = grpc.makeClientConstructor(healthService, "Health");
    const healthClient = new HealthClient(host, grpc.credentials.createInsecure()) as unknown as grpc.Client & {
        check(
            request: { service: string },
            options: grpc.CallOptions,
            callback: (err: grpc.ServiceError | null, res?: { status: string | number }) => void,
        ): void;
    };

    // Throws an UNAVAILABLE RpcError if the server is down or not SERVING
    function checkHealth(serviceName = "streaming.StreamingService"): Promise<void> {
        return new Promise((resolve, reject) => {
            healthClient.check({ service: serviceName }, { deadline: Date.now() + 2000 }, (err, res) => {
                if (err) {
                    return reject(new RpcError(`Health check failed: ${err.details}`, "UNAVAILABLE"));
                }
                if (res?.status !== "SERVING" && res?.status !== 1) {
                    return reject(new RpcError(`Service not serving (${res?.status})`, "UNAVAILABLE"));
                }
                resolve();
            });
        });
    }

    async function receive(stream: SensorStream, commands: ServerCommand[]) {
        for await (const command of stream.responses) {
            log("Command from server:", command);
            commands.push(command);
            if (command.command === Command.REBOOT) {
                log("Reboot command received from server");
            }
        }
    }

    async function send(stream: SensorStream) {
        for (let i = 0; i < readings; i++) {
            const reading = {
                deviceId: "sensor-001",
                temperature: 20 + i * 4,
                humidity: 40,
                status: Status.OK,
            };
            log("Sending:", reading);
            await stream.requests.send(reading);
            await sleep(sendInterval);
        }
        await stream.requests.complete();
    }

    // One attempt: opens a fresh stream, sends and receives until the server ends it
    async function runOnce(authToken: string): Promise<ServerCommand[]> {
        // No connection / not healthy: fail here, before anything is sent
        await checkHealth();

        const stream = client.streamSensorReadings({
            meta: { authorization: authToken, "x-client-id": "sensor-001" },
        });

        const commands: ServerCommand[] = [];
        await Promise.all([receive(stream, commands), send(stream)]);

        // The headers promise never settles if the server answered without a
        // message (trailers-only response), so do not wait for it past the status
        const status = await stream.status;
        const headers = await Promise.race([stream.headers, Promise.resolve(undefined)]);
        log("headers", headers);
        log("trailers", await stream.trailers);
        log("status", status);
        return commands;
    }

    // Retries with backoff; resolves with the commands, or throws the final RpcError
    async function runWithRetry(): Promise<ServerCommand[]> {
        let refreshed = false;
        for (let attempt = 1; ; attempt++) {
            try {
                return await runOnce(token);
            } catch (err) {
                if (!(err instanceof RpcError)) throw err;
                log(`gRPC error ${err.code}:`, err.message);

                // Token expired or wrong: renew once and retry immediately
                if (err.code === "UNAUTHENTICATED" && !refreshed) {
                    token = await fetchNewToken();
                    refreshed = true;
                    continue;
                }

                if (!RETRYABLE.has(err.code) || attempt >= maxAttempts) {
                    log("Giving up:", err.code, err.message);
                    throw err;
                }

                const delay = Math.min(delayBase * 2 ** (attempt - 1), 30000) * (0.5 + Math.random());
                log(`Attempt ${attempt} failed (${err.code}), new attempt in ${Math.round(delay)} ms`);
                await sleep(delay);
            }
        }
    }

    function close() {
        healthClient.close();
        transport.close();
    }

    return { checkHealth, runOnce, runWithRetry, close };
}

// Only run when started directly (pnpm run runClient), not when imported by tests
if (import.meta.main) {
    const sensor = createSensorClient({
        host: "localhost:50051",
        token: "******",
        fetchNewToken: () => Promise.resolve("******"),
    });
    try {
        await sensor.runWithRetry();
    } catch {
        process.exitCode = 1;
    } finally {
        sensor.close();
    }
}
