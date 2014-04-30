/**
 * @author: mike@arsenicsoup.com
 * @since: March 3rd, 2014
 */

var Logger = require('arsenic-logger');


Topology = (function() {

    var self = this;

    // class function a.k.a. constructor
    return function(spouts, bolts, outputs, inputs) {

        var workers = [];
        var nworkers = 0;
        var topology = this;
        
        var server;
        var connected;
        var listener;
        var local;

        if (inputs) {
            for (var n in inputs) {
                inputs[n].topology = this;
            }
        }

        // ////////////////////////////////////////////////////////////////////////////

        this.start = function() {
            for (var n in spouts) {
                var spout = spouts[n];
                spout.start(outputs[n]);
            }
        };

        // ////////////////////////////////////////////////////////////////////////////

        this.stop = function() {

            if (server){
                server.end();
            }

            if (listener){
                listener.close();
            }

            workers.forEach(function(worker) {
                worker.end();
            });
        };
    
    }


})();

module.exports = Topology;
