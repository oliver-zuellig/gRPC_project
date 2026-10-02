import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { HealthImplementation } from 'grpc-health-check';
import type { ServingStatusMap } from 'grpc-health-check';
import { ReflectionService } from '@grpc/reflection';

const packageDefinition = protoLoader.loadSync("./proto/telemetry.proto");

const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const statusMap: ServingStatusMap = {
    TelemetryService: 'SERVING',
    '': 'SERVING',
};

enum SensorState {
    SENSOR_STATE_UNSPECIFIED = 0,
    SENSOR_STATE_OK = 1,
    SENSOR_STATE_WARNING = 2,
    SENSOR_STATE_CRITICAL = 3
}

function sendReading(
    call: any,
    callback: any
) {
    console.log("Reading received");

    console.log(call.request);

    console.log(call.request.timestamp.seconds.toString());
    console.log(SensorState[call.request.state]);

    callback(null, {
        success: true
    });
}

const server = new grpc.Server();

server.addService(
    proto.telemetry.TelemetryService.service,
    {
        SendReading: sendReading,
    }
);

const health = new HealthImplementation(statusMap);

health.addToServer(server);


// for example if DB is not serving, use setStatus('','NOT_SERVING')

//Reflection
const reflectionAPI = new ReflectionService(packageDefinition);

reflectionAPI.addToServer(server);

server.bindAsync(
    "0.0.0.0:50051",
    grpc.ServerCredentials.createInsecure(),
    () => {
        console.log("Server running");
    }
);
