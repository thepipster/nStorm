exports.createTopologyServer = function () { 

    var clients = [];
    
    function removeClient(client) {
        var position = clients.indexOf(client);
        
        delete clients[position];
    }
    
    function addClient(client) {
        clients.push(client);
    }
    
    function broadcast(msg, sender) {
        clients.forEach(function (client) {
            if (client !== sender)
                client.write(msg);
        });
    }
    
    return simplemessages.createServer(function (client) {
        addClient(client);

        client.on('error', function() { removeClient(client); });
        client.on('close', function() { removeClient(client); });
        client.on('data', function(msg) {
            if (isHostMessage(msg))
                broadcast(msg, client);
        });
    });
}
