import * as grpc from "@grpc/grpc-js";
import { streamingServiceDefinition } from "../generated/proto/streaming.grpc-server";
import type { IStreamingService } from "../generated/proto/streaming.grpc-server";
import { Command } from "../generated/proto/streaming";
import { HealthImplementation } from "grpc-health-check";

export const VALID_TOKEN = "******";

const impl: IStreamingService = {
    streamSensorReadings(call: grpc.ServerDuplexStream<any, any>) {
        const token = call.metadata.get("authorization")[0];
        if (token !== VALID_TOKEN) {
            call.emit("error", { code: grpc.status.UNAUTHENTICATED, message: "Ungültiges Token" });
            return;
        }
        call.on("data", (reading) => {
            if (reading.temperature > 30) {
                call.write({ command: Command.REBOOT, interval: 0, calibrationValue: 0 });
            }
        });
        call.on("end", () => call.end());
    },
};

export function createServer() {
    const server = new grpc.Server();
    server.addService(streamingServiceDefinition, impl);

    // Health service: '' = whole server, plus our own service by its full name
    const health = new HealthImplementation({
        "": "SERVING",
        "streaming.StreamingService": "SERVING",
    });
    health.addToServer(server);
    return { server, health };
}

// Port 0 = pick a free port (used by tests); resolves with the real port
export function listen(server: grpc.Server, address: string): Promise<number> {
    return new Promise((resolve, reject) => {
        server.bindAsync(address, grpc.ServerCredentials.createInsecure(), (err, port) =>
            err ? reject(err) : resolve(port),
        );
    });
}

// Only start when run directly (pnpm run runServer), not when imported by tests
if (import.meta.main) {
    const { server } = createServer();
    const port = await listen(server, "0.0.0.0:50051");
    console.log(`Server runs on Port ${port}`);
}

/* 

const responseHeaders = new grpc.Metadata();

// by default, the server always writes some custom response headers
if (!call.request.disableSendingExampleResponseHeaders) {
    responseHeaders.add('server-header', 'server header value');
    responseHeaders.add('server-header', 'server header value duplicate');
    responseHeaders.add('server-header-bin', Buffer.from('server header binary value'));
}
call.sendMetadata(responseHeaders);


*/

