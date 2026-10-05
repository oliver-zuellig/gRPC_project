import * as grpc from "@grpc/grpc-js";
import { streamingServiceDefinition } from "../generated/proto/streaming.grpc-server";
import type { IStreamingService } from "../generated/proto/streaming.grpc-server";
import { Command } from "../generated/proto/streaming";
import { HealthImplementation } from "grpc-health-check";

const impl: IStreamingService = {
    streamSensorReadings(call: grpc.ServerDuplexStream<any, any>) {
        const token = call.metadata.get("authorization")[0];
        if (token !== "Bearer abc") {
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

const server = new grpc.Server();
server.addService(streamingServiceDefinition, impl);

// Health service: '' = whole server, plus our own service by its full name
const health = new HealthImplementation({
    "": "SERVING",
    "streaming.StreamingService": "SERVING",
});
health.addToServer(server);
server.bindAsync(
    "0.0.0.0:50051",
    grpc.ServerCredentials.createInsecure(),
    (err, port) => {
        if (err) throw err;
        console.log(`Server runs on Port ${port}`);
    }
);

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

