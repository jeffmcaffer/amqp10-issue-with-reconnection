module.exports = {createConnectionString}

function createConnectionString() {
    if (process.env.AMQP_URL) {
        return process.env.AMQP_URL;
    }
    var protocol = 'amqps';
    var serviceBusHost = process.env.SERVICE_BUS_HOST + '.servicebus.windows.net';
    if (process.env.SERVICE_BUS_HOST.indexOf(".") !== -1) {
        serviceBusHost = process.env.SERVICE_BUS_HOST;
    }
    var sasName = process.env.SAS_KEY_NAME;
    var sasKey = process.env.SAS_KEY;

    var uri = protocol + '://' + encodeURIComponent(sasName) + ':' + encodeURIComponent(sasKey) + '@' + serviceBusHost;

    return uri;
}
