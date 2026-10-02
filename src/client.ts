import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import HealthCheck from 'grpc-health-check';

const packageDefinition = protoLoader.loadSync("./proto/telemetry.proto");

const proto = grpc.loadPackageDefinition(packageDefinition) as any;

const client = new proto.telemetry.TelemetryService(
    "localhost:50051",
    grpc.credentials.createInsecure()
);

enum SensorState {
    SENSOR_STATE_UNSPECIFIED = 0,
    SENSOR_STATE_OK = 1,
    SENSOR_STATE_WARNING = 2,
    SENSOR_STATE_CRITICAL = 3
}

let sendState  = SensorState.SENSOR_STATE_UNSPECIFIED;
const sendTimeObj = {
        seconds: Date.now(),
        nanos: 0
    }
if(sendTimeObj.seconds % 2 == 0){
    sendState = SensorState.SENSOR_STATE_OK;
} else {
    sendState = SensorState.SENSOR_STATE_WARNING;
}

client.SendReading(
    
    {
        deviceId: "sensor-001",
        temperature: 22.5,
        timestamp: sendTimeObj,
        state: sendState
    },
    (err: any, response: any) => {
        console.log('err', err);
        console.log('response', response);
    }
);


const HealthClientConstructor = grpc.makeClientConstructor(HealthCheck.service, 'grpc.health.v1.HealthService');
const healthClient = new HealthClientConstructor("localhost:50051", grpc.credentials.createInsecure()) as any;
healthClient.Check({ service: '' },(error:any, value:any)=>{
    console.log('error',error);
    console.log('value',value);
});