require("console-stamp")(console);
//load env'
require('dotenv').config();
var amqp = require('amqp10');
var AMQPClient = require('amqp10').Client;
var Policy = require('amqp10').Policy;
var utils = require("./utils");
var queueName = process.env.QUEUE_NAME || 'queue';

var connectstring = utils.createConnectionString();
var client = new AMQPClient(Policy.ServiceBusQueue);

client.on(AMQPClient.ErrorReceived, function(err){
    console.log('error received on client', err)
});


client.on(AMQPClient.ConnectionOpened, function(err){
    console.log('connection openened');
});

client.on(AMQPClient.ConnectionClosed, function(err){
    console.log('connection closed');
});

client.on('connected', function(err){
    console.log('connected');
});




var connectPromise = client.connect(connectstring);

connectPromise.then(setupSender).catch(err => console.error(err));


function setupSender() {
    console.log('setup sender');
    function startSendingMessages(sender) {
      for (let i = 0; i < 10; i++) {
        console.log('sender created ' + i);
        setInterval(function () {
          for (let j = 0; j < 10; j++) {
            sendMessage(sender, i);
          }
        }, 100);
      }
    }

    client.createSender('queue').then(startSendingMessages).catch(errorCallback);
}

var messageCount = [];
function sendMessage(sender, loop) {
    messageCount[loop] = messageCount[loop] || {attempted: 0, completed: 0};
    var payload = "abc";
    messageCount[loop].attempted++;
    sender.send(payload).then(function () {
        messageCount[loop].completed++;

        if (messageCount[loop].attempted % 1000 === 0){
            console.log(`attempted 1.000 message, completed ${messageCount[loop].completed} to the queue in loop ${loop}`);
        }
    }).catch(errorCallback);
}

function errorCallback(err) {
    console.error("error while sending", err);
    console.error("failed sending message,", err);
};




