"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const ws_1 = require("ws");
const redis_1 = require("redis");
const publishClient = (0, redis_1.createClient)();
publishClient.connect();
const subscribeClient = (0, redis_1.createClient)();
subscribeClient.connect();
const wss = new ws_1.WebSocketServer({ port: 8081 });
const subscriptions = {};
// setInterval(() => {
//     console.log(subscriptions);
// }, 5000);
wss.on("connection", function connection(userSocket) {
    const id = randomId();
    subscriptions[id] = {
        ws: userSocket,
        rooms: [],
    };
    userSocket.on("message", function message(data) {
        const parsedMessage = JSON.parse(data);
        if (parsedMessage.type === "SUBSCRIBE") {
            subscriptions[id].rooms.push(parsedMessage.room);
            if (oneUserSubscribedTo(parsedMessage.room)) {
                console.log("subscribing on the pub sub to room " + parsedMessage.room);
                subscribeClient.subscribe(parsedMessage.room, (message) => {
                    const parsedMessage = JSON.parse(message);
                    Object.keys(subscriptions).forEach((userId) => {
                        const { ws, rooms } = subscriptions[userId];
                        if (rooms.includes(parsedMessage.roomId)) {
                            ws.send(parsedMessage.message);
                        }
                    });
                });
            }
        }
        if (parsedMessage.type === "UNSUBSCRIBE") {
            subscriptions[id].rooms = subscriptions[id].rooms.filter((x) => x !== parsedMessage.room);
            if (lastPersonLeftRoom(parsedMessage.room)) {
                console.log("unsubscribing from pub sub on room" + parsedMessage.room);
                subscribeClient.unsubscribe(parsedMessage.room);
            }
        }
        if (parsedMessage.type === "sendMessage") {
            const message = parsedMessage.message;
            const roomId = parsedMessage.roomId;
            publishClient.publish(roomId, JSON.stringify({
                type: "sendMessage",
                roomId: roomId,
                message,
            }));
        }
    });
});
function oneUserSubscribedTo(roomId) {
    let totalInterestedPeople = 0;
    Object.keys(subscriptions).map((userId) => {
        if (subscriptions[userId].rooms.includes(roomId)) {
            totalInterestedPeople++;
        }
    });
    if (totalInterestedPeople == 1) {
        return true;
    }
    return false;
}
function lastPersonLeftRoom(roomId) {
    let totalInterestedPeople = 0;
    Object.keys(subscriptions).map((userId) => {
        if (subscriptions[userId].rooms.includes(roomId)) {
            totalInterestedPeople++;
        }
    });
    if (totalInterestedPeople == 0) {
        return true;
    }
    return false;
}
function randomId() {
    return Math.random();
}
