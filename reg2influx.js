const mqttReg = require("@device.farm/mqtt-reg");
const { InfluxDB, Point } = require("@influxdata/influxdb-client");

const log = {
    error: require("debug")("app:error"),
    info: require("debug")("app:info"),
    debug: require("debug")("app:debug")
}

const VALUE_FIELD_NAME = "value";

require("@device.farm/appglue")({ require, file: __dirname + "/config.json" }).main(async ({
    mqtt,
    delaySec,
    influx
}) => {

    const client = new InfluxDB({ url: influx.host, token: influx.token });
    const writeApi = client.getWriteApi(influx.org, influx.bucket)

    let regs = {};

    mqttReg.mqttAdvertise(mqtt, name => {
        if (!regs[name]) {
            log.info("Adding register", name);
            regs[name] = mqttReg.mqttReg(mqtt, name, (value, prev, initial) => {
                let reg = regs[name]; 
                if (reg) {
                    reg.timestamp = new Date();
                    let value = reg.actual();
                    reg.value = value;
                }
            });
        }
    });

    while (true) {
        try {
            await new Promise(resolve => setTimeout(resolve, delaySec * 1000));
            for (let name in regs) {

                let value = regs[name].actual();
                let timestamp = regs[name].timestamp;

                if (value !== undefined && timestamp !== undefined) {

                    log.debug(name, value, timestamp);

                    const point = new Point(name);
                    point.timestamp(timestamp);
                    let tokens = name.split(".");
                    for (let i in tokens) {
                        point.tag("t" + (parseInt(i) + 1), tokens[i]);
                    }

                    if (typeof value === "boolean") {
                        point.booleanField(VALUE_FIELD_NAME, value);
                    } else if (typeof value === "number") {
                        point.floatField(VALUE_FIELD_NAME, value);
                    } else if (typeof value === "string") {
                        point.stringField(VALUE_FIELD_NAME, value);
                    } else {
                        continue;
                    }

                    writeApi.writePoint(point);
                }

            }

            await writeApi.flush();

        } catch (e) {
            log.error("Error in the loop:", e);
        }
    }

});